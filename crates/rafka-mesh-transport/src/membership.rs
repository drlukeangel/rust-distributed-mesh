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
use crate::forward_path::ForwardPath;
use crate::snapshot::{Chunk, Delta, Forwarder, Full, Gap, Install, Moved, PublisherId, SnapshotReceiver, SourceSnapshot, SourceVersion, Taken};
use anyhow::Result;
use bytes::Bytes;
use futures_lite::StreamExt as _;
use iroh::address_lookup::MemoryLookup;
use iroh::{Endpoint, EndpointAddr};
use iroh_gossip::api::{Event, GossipSender};
use iroh_gossip::net::Gossip;
use iroh_gossip::proto::TopicId;
use rafka_mesh_entity::seat::transfer::CommittedSeat;
use rafka_mesh_entity::{FabricId, IncarnationId, LifecycleOp, MeshDigest, MeshId, NodeId, RoundHook, Seat, SeatHolder, DEPARTED_RETENTION};
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

/// Tell `endpoint` where `peers` are (no discovery service runs): a lookup of its own that only
/// adds. For a raw gossip observer with no [`Membership`]; an endpoint that holds a
/// [`LocationBook`] installs every address through that book.
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
/// `peer_location_wall_time`), and the socket that birth named: governs the address book only,
/// never the held view. Ordered as the held view is: by `digest_seq` within one incarnation, and
/// along the lineage across incarnations.
type LocationMark = (rafka_mesh_entity::IncarnationId, u64, Option<rafka_mesh_entity::IncarnationId>, std::net::SocketAddr);

/// The marks of one peer-location book, by key. Each book holds its own: a newer birth one node of
/// a process consumed is still newer to every other node of that process.
pub type LocationMarks = Mutex<HashMap<iroh::PublicKey, LocationMark>>;

/// The peer-location book of one endpoint: the ONE address book its address lookup reads. Every
/// channel and topic the endpoint joins installs where a key is through it, and only a birth's
/// ordered digest changes a key's address (gossip.md, contact fields: "Contact changes continue
/// through the birth's ordered digest updates"). A move retires every other direct path the
/// endpoint holds to the key (R-I1, `Endpoint::replace_direct_addrs`). An address from anything
/// else (a seed, a peer list) fills a key the book holds no address for and never adds a second
/// one: iroh merges every address lookup service's answer when it dials a key with no selected
/// path, so a socket kept in a second book is dialled again after its key moved.
#[derive(Clone)]
pub struct LocationBook {
    lookup: MemoryLookup,
    endpoint: Endpoint,
    marks: Arc<LocationMarks>,
    /// The node this book belongs to: the observer every location span names.
    observer: String,
}

impl LocationBook {
    /// The book of `endpoint`, held by `observer`, installed as an address lookup of the endpoint.
    pub fn new(endpoint: &Endpoint, observer: &str) -> Result<Self> {
        let lookup = MemoryLookup::new();
        endpoint.address_lookup().map_err(|e| anyhow::anyhow!("address_lookup: {e}"))?.add(lookup.clone());
        Ok(Self { lookup, endpoint: endpoint.clone(), marks: Arc::default(), observer: observer.to_string() })
    }

    /// Register where the birth `d` names is, and retire the paths a moved key leaves behind.
    pub fn register(&self, d: &MeshDigest) {
        retire_superseded(&self.endpoint, &self.observer, register_location(&self.marks, &self.lookup, d));
    }

    /// Fill `addr` for its key when the book holds no address for that key; an address the book
    /// holds is changed only by a newer birth's digest ([`LocationBook::register`]).
    pub fn fill(&self, addr: &EndpointAddr) {
        if self.lookup.get_endpoint_info(addr.id).is_none_or(|e| e.ip_addrs().next().is_none()) {
            self.lookup.add_endpoint_info(addr.clone());
        }
    }

    /// Forget every address of `key`: a birth under an open restart or removal is never handed out.
    pub fn forget(&self, key: iroh::PublicKey) {
        self.lookup.remove_endpoint_info(key);
    }

    /// The direct addresses this book holds for `key`.
    pub fn addrs(&self, key: iroh::PublicKey) -> Vec<std::net::SocketAddr> {
        self.lookup.get_endpoint_info(key).map(|e| e.ip_addrs().cloned().collect()).unwrap_or_default()
    }
}

/// The `(node.id, incarnation)` pairs a `node.deleted` fenced in the peer-location book: a digest of
/// one never registers its socket for its key. A new node over a deleted one's data dir keeps the
/// key and sets no `supersedes`, so lineage alone cannot refuse the deleted instance's late digest.
fn fenced_incarnations() -> &'static Mutex<std::collections::HashSet<(String, String)>> {
    static F: std::sync::OnceLock<Mutex<std::collections::HashSet<(String, String)>>> = std::sync::OnceLock::new();
    F.get_or_init(Mutex::default)
}

/// Fence the exact birth `(node_id, incarnation)` in the peer-location book.
pub fn fence_incarnation(node_id: &NodeId, incarnation: &rafka_mesh_entity::IncarnationId) {
    fenced_incarnations().lock().unwrap().insert((node_id.to_string(), incarnation.0.clone()));
}

/// Whether `(node_id, incarnation)` is fenced in the peer-location book.
fn incarnation_fenced(node_id: &NodeId, incarnation: &rafka_mesh_entity::IncarnationId) -> bool {
    fenced_incarnations().lock().unwrap().contains(&(node_id.to_string(), incarnation.0.clone()))
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
/// Returns the key, the socket it moved from and the new socket when a newer birth MOVED the key:
/// the caller then retires the endpoint's paths to every other socket (`retire_superseded`). The
/// socket it moved from is the one the book last registered for the key, or else the one it holds:
/// a book that forgot the key under an open restart still knows where the key was.
pub fn register_location(marks: &LocationMarks, lookup: &MemoryLookup, d: &MeshDigest) -> Option<LocationMove> {
    let addr = gossip_addr(d)?;
    let key = addr.id;
    if incarnation_fenced(&d.node.node_id, &d.node.incarnation) {
        tracing::info_span!(
            "rdm.mesh.connection.reject.via-fenced-incarnation",
            node = %d.node.name,
            node_id = %d.node.node_id,
            incarnation_id = %d.node.incarnation.0,
            socket = %d.node.transport_addr,
        )
        .in_scope(|| tracing::info!("a digest of a deleted incarnation: its socket is not registered for the key"));
        return None;
    }
    let (fresher, marked) = {
        let mut w = marks.lock().unwrap();
        let marked = w.get(&key).map(|m| m.3);
        let fresher = match w.get(&key) {
            None => true,
            Some((incarnation, seq, _, _)) if *incarnation == d.node.incarnation => d.digest_seq > *seq,
            Some((_, _, supersedes, _)) => supersedes.as_ref() != Some(&d.node.incarnation),
        };
        if fresher {
            w.insert(key, (d.node.incarnation.clone(), d.digest_seq, d.node.supersedes.clone(), d.node.transport_addr));
        }
        (fresher, marked)
    };
    let held = lookup.get_endpoint_info(key).and_then(|e| e.ip_addrs().next().cloned());
    let mut moved = None;
    if fresher {
        if let Some(old) = marked.or(held).filter(|o| *o != d.node.transport_addr) {
            moved = Some(LocationMove { key, node: d.node.name.to_string(), incarnation: d.node.incarnation.0.clone(), old, new: d.node.transport_addr });
        }
        lookup.set_endpoint_info(addr);
    } else if held.is_none() {
        lookup.add_endpoint_info(addr);
    }
    moved
}

/// A newer birth of `key` (node `node`, birth `incarnation`) moved it from `old` to `new`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocationMove {
    /// The key that moved.
    pub key: iroh::PublicKey,
    /// The node the newer birth names.
    pub node: String,
    /// The newer birth.
    pub incarnation: String,
    /// The socket the book held for the key before.
    pub old: std::net::SocketAddr,
    /// The socket the newer birth names.
    pub new: std::net::SocketAddr,
}

/// A newer birth moved a key: the endpoint retires every direct path it holds to any other socket
/// of the key, open ones included (iroh `Endpoint::replace_direct_addrs`), so a connection to the
/// old socket, or a dial's first packet to it, can no longer reach whoever took that port. The span
/// ends when the endpoint has retired them (`retired=true`).
fn retire_superseded(endpoint: &Endpoint, observer: &str, moved: Option<LocationMove>) {
    let Some(m) = moved else { return };
    let span = tracing::info_span!(
        "rdm.mesh.connection.update.via-peer-location-refreshed",
        observer = %observer,
        node = %m.node,
        incarnation_id = %m.incarnation,
        peer = %m.key.fmt_short(),
        old_socket = %m.old,
        new_socket = %m.new,
        retired = tracing::field::Empty,
    );
    span.in_scope(|| tracing::info!("a newer birth of this key moved: its old socket is no longer dialled"));
    let endpoint = endpoint.clone();
    tokio::spawn(tracing::Instrument::instrument(
        async move {
            endpoint.replace_direct_addrs(m.key, [m.new]).await;
            tracing::Span::current().record("retired", true);
            tracing::info!("every direct path to another socket of the key is retired");
        },
        span,
    ));
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
        /// The members this chunk carries, each a member projection entry: its digest, then the
        /// status its mesh-primary holds (gossip.md §3.1).
        #[serde(with = "crate::wire::entries")]
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
        /// The births added or changed, each a member projection entry: its digest, then the status
        /// its mesh-primary holds (gossip.md §3.1).
        #[serde(with = "crate::wire::entries")]
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
    /// Published on the executor's own mesh channel only; the backbone carries it as an open
    /// overlay of its mesh's aggregate (`Members.in_flight`).
    NodeDeleting {
        /// The removal being executed.
        op: LifecycleOp,
        /// The local primary that forwards the frame onto its own channel.
        forwarded_by: Option<String>,
        /// The exact birth that published the fact: the epoch of `topology_version`. A receiver
        /// takes it only from a publisher it holds seated for the subject's mesh.
        publisher: PublisherId,
        /// The publisher's `topology_version` of the subject's mesh at which the subject changed.
        topology_version: u64,
    },
    /// The provider proved `op`'s exact birth terminal: it has left. Same
    /// channel as `NodeDeleting`; retained afterwards in `Members.departed`.
    NodeDeleted {
        /// The removal that completed.
        op: LifecycleOp,
        /// The local primary that forwards the frame onto its own channel.
        forwarded_by: Option<String>,
    },
    /// The mesh executor holding `op` (`restart-node:<path>`) is restarting its exact birth:
    /// commanded silence (fabric-node-lifecycle.md: node-admin took it down and owns bringing it
    /// back). The node stays held through its own `Leaving` and silence, not routable, until a
    /// later birth of the same NodeId is heard. Same channel as `NodeDeleting`; carried in
    /// `Members.in_flight` while open.
    NodeRestarting {
        /// The restart being executed.
        op: LifecycleOp,
        /// The local primary that forwards the frame onto its own channel.
        forwarded_by: Option<String>,
        /// The exact birth that published the fact: the epoch of `topology_version`, as on
        /// [`Frame::NodeDeleting`].
        publisher: PublisherId,
        /// The publisher's `topology_version` of the subject's mesh at which the subject changed.
        topology_version: u64,
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
    /// The owning mesh-admin accepted `drain-node:<path>` for the exact birth in `op`
    /// (gossip.md, node-draining): drain intent, never completion. The command hook publishes it
    /// on the admin's own mesh channel and the backbone; a peer mesh's primary forwards it onto its
    /// own channel (`forwarded_by`).
    NodeDraining {
        /// The accepted drain command.
        op: LifecycleOp,
        /// The local primary that forwards the frame onto its own channel.
        forwarded_by: Option<String>,
    },
    /// The owning mesh-admin accepted `stop-node:<path>` for the exact birth in `op`
    /// (gossip.md, node-leaving): stop intent, never exit proof. Same channels as
    /// [`Frame::NodeDraining`].
    NodeLeaving {
        /// The accepted stop command.
        op: LifecycleOp,
        /// The local primary that forwards the frame onto its own channel.
        forwarded_by: Option<String>,
    },
    /// The exact birth in `op` sent its `node-left` completion (gossip.md, node-left): a
    /// check-in, not exit proof. The node publishes it on its own mesh channel; its owning
    /// mesh-admin carries it onto the backbone preserving `op` and naming itself in
    /// `forwarded_by`; a peer mesh's primary forwards it onto its own channel.
    NodeLeft {
        /// The stop operation and the exact birth that left.
        op: LifecycleOp,
        /// The admin or primary that forwards the frame.
        forwarded_by: Option<String>,
    },
    /// The exact birth in `op` sent its `node-drained` completion (gossip.md, node-drained): no
    /// eligible work remains in flight and the process keeps running. Published by the node on
    /// its own mesh channel; carried and forwarded exactly as [`Frame::NodeLeft`].
    NodeDrained {
        /// The drain operation and the exact birth that drained.
        op: LifecycleOp,
        /// The admin or primary that forwards the frame.
        forwarded_by: Option<String>,
    },
    /// The fabric-primary accepted `leave-mesh` for `mesh_id` (gossip.md, mesh-leaving): leave
    /// intent, never exit proof. Published by the leave-mesh command hook on the backbone and the
    /// fabric-primary's own mesh channel; a peer mesh's primary forwards it onto its own channel.
    MeshLeaving {
        /// The mesh that leaves.
        mesh_id: MeshId,
        /// The Build the operation belongs to.
        build_id: String,
        /// The Build attempt that holds the operation.
        attempt: u32,
        /// `shutdown-mesh:<mesh_id>`.
        operation: String,
        /// The fabric-primary that authored the frame.
        publisher: String,
        /// The Rafka-time of the event; evidence only.
        event_at_rafka_ms: u64,
        /// The local primary that forwards the frame onto its own channel.
        forwarded_by: Option<String>,
    },
    /// The leaving mesh's still-running primary handed its member receipts to the fabric-primary
    /// (gossip.md, mesh-leave): handoff evidence, not completed departure; the final primary keeps
    /// running. Published on the backbone and the primary's own mesh channel.
    MeshLeave {
        /// The mesh that leaves.
        mesh_id: MeshId,
        /// The Build the operation belongs to.
        build_id: String,
        /// The Build attempt that holds the operation.
        attempt: u32,
        /// `shutdown-mesh:<mesh_id>`.
        operation: String,
        /// The final mesh-primary's node.
        final_node_id: NodeId,
        /// The final mesh-primary's incarnation.
        final_incarnation: IncarnationId,
        /// The reference to the other members' terminal receipts.
        receipt_manifest: String,
        /// The mesh-primary that authored the frame.
        publisher: String,
        /// The Rafka-time of the event; evidence only.
        event_at_rafka_ms: u64,
        /// The local primary that forwards the frame onto its own channel.
        forwarded_by: Option<String>,
    },
    /// The fabric-primary recorded the completed mesh departure (gossip.md, mesh-left): every
    /// accepted birth, the final primary included, was proven exited. Published on the backbone and
    /// the fabric-primary's own mesh channel; a peer mesh's primary forwards it onto its own.
    MeshLeft {
        /// The mesh that left.
        mesh_id: MeshId,
        /// The Build the operation belongs to.
        build_id: String,
        /// The Build attempt that holds the operation.
        attempt: u32,
        /// `shutdown-mesh:<mesh_id>`.
        operation: String,
        /// The reference to the complete terminal proof set.
        receipt_manifest: String,
        /// The fabric-primary that authored the frame.
        publisher: String,
        /// The Rafka-time of the event; evidence only.
        event_at_rafka_ms: u64,
        /// The local primary that forwards the frame onto its own channel.
        forwarded_by: Option<String>,
    },
    /// The committed fabric-primary announces itself and its contact addresses on the backbone
    /// (gossip.md, new-fabric-primary), after its explicit handover committed the fabric seat. The
    /// contacts belong to the exact `holder` birth and nothing else; the frame grants no authority
    /// beyond the committed record it carries, completes no handover and proves no runtime exit.
    NewFabricPrimary {
        /// The committed holder: mesh, node, incarnation and the seat's new epoch.
        holder: SeatHolder,
        /// The holder's endpoint key.
        endpoint_id: String,
        /// The holder's bound QUIC UDP address.
        transport_addr: std::net::SocketAddr,
        /// The holder's control API base URL with its bound HTTP port, when it runs one.
        admin_api_base: Option<String>,
    },
    /// The commanding authority sent `commit-state:<fabric_id>` to the exact birth in `hook`
    /// (gossip.md, "Fabric round gossip hooks"): command intent, never completion. A node-admin
    /// author publishes it on its own mesh channel and the backbone; a peer mesh's primary forwards
    /// it onto its own channel (`forwarded_by`), the authored fields unchanged.
    StateCommitting {
        /// The round, the subject birth and the author.
        hook: RoundHook,
        /// The primary that forwards the frame onto its own channel.
        forwarded_by: Option<String>,
    },
    /// The exact birth in `hook` sent its `state-committed` check-in after its scratchpad writes were
    /// stored: a phase check-in, never a node or mesh lifecycle state. A node-admin publishes it on
    /// its own mesh channel and the backbone; an ordinary node on its own mesh channel, and its
    /// mesh-admin carries it onto the backbone naming itself in `forwarded_by`.
    StateCommitted {
        /// The round, the subject birth and the author.
        hook: RoundHook,
        /// The admin or primary that forwards the frame.
        forwarded_by: Option<String>,
    },
    /// The commanding authority sent `open-traffic:<fabric_id>` to the exact birth in `hook`; same
    /// channels as [`Frame::StateCommitting`].
    TrafficOpening {
        /// The round, the subject birth and the author.
        hook: RoundHook,
        /// The primary that forwards the frame onto its own channel.
        forwarded_by: Option<String>,
    },
    /// The exact birth in `hook` sent its `traffic-opened` check-in; same channels and carrying as
    /// [`Frame::StateCommitted`].
    TrafficOpened {
        /// The round, the subject birth and the author.
        hook: RoundHook,
        /// The admin or primary that forwards the frame.
        forwarded_by: Option<String>,
    },
    /// The mesh executor holding `op` (`stop-node:<path>`) received the exact birth's `left` on the
    /// stop call's own reply: the birth is parked (its process alive, its endpoint bound) and
    /// gossips nothing. Every node drops it from its book until the birth itself rejoins. Same
    /// channel as `NodeDeleting`; carried in `Members.in_flight` while open.
    NodeStopped {
        /// The stop being executed.
        op: LifecycleOp,
        /// The local primary that forwards the frame onto its own channel.
        forwarded_by: Option<String>,
        /// The exact birth that published the fact: the epoch of `topology_version`, as on
        /// [`Frame::NodeDeleting`].
        publisher: PublisherId,
        /// The publisher's `topology_version` of the subject's mesh at which the subject changed: a
        /// projection of that publisher newer than it, listing the birth, moved past the stop.
        topology_version: u64,
    },
    /// A seat holder committed a Build-driven seat transfer (gossip.md, "Seated-together seat
    /// transfer"): its one seat row moved every seat in `seats` to `successor`, the executor of the
    /// drain, stop, restart or replace `op` names. The holder publishes it on its own mesh channel
    /// and the backbone before it answers SeatsCommitted; peer mesh primaries carry it unchanged.
    /// Every node-admin holds the seats together; it announces committed leadership and opens no
    /// executor gate (the SeatsCommitted reply does).
    SeatedTogether {
        /// The operation the transfer precedes; its subject is the former holder.
        op: LifecycleOp,
        /// The committed holder.
        successor: SeatHolder,
        /// Every seat the row moved, at its committed epoch.
        seats: Vec<CommittedSeat>,
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

/// The status events (node-status-heartbeat-connections.md R-HB1, "Events vs statuses": "NodeStopped,
/// NodeDeleting and NodeRestarting stay immediate events"): each carries its publisher and the
/// `topology_version` at which its subject changed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusEvent {
    /// `NodeDeleting`: the executor started removing the exact birth.
    Deleting,
    /// `NodeRestarting`: the executor is restarting the exact birth.
    Restarting,
    /// `NodeStopped`: the executor holds the exact birth's matching stopped reply.
    Stopped,
}

impl StatusEvent {
    /// The frame's name in spans.
    pub fn name(self) -> &'static str {
        match self {
            Self::Deleting => "node-deleting",
            Self::Restarting => "node-restarting",
            Self::Stopped => "node-stopped",
        }
    }

    /// The frame of this event for `op`, published by `publisher` at `topology_version`.
    pub fn frame(self, op: LifecycleOp, publisher: PublisherId, topology_version: u64) -> Frame {
        match self {
            Self::Deleting => Frame::NodeDeleting { op, forwarded_by: None, publisher, topology_version },
            Self::Restarting => Frame::NodeRestarting { op, forwarded_by: None, publisher, topology_version },
            Self::Stopped => Frame::NodeStopped { op, forwarded_by: None, publisher, topology_version },
        }
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

/// The mesh status a `Frame::MeshStatus` carries for a ready-for-traffic mesh (states.md: a mesh is
/// `pending` -> `ready-for-traffic` -> `leaving` -> `dead`).
pub const MESH_READY_FOR_TRAFFIC: &str = "ready-for-traffic";

/// The statuses a node holds, each until the next change: a mesh's by its name, the fabric's. A
/// frame older than the one held is refused by name, the same one again is taken once.
#[derive(Debug, Clone)]
pub struct StatusBook {
    meshes: Arc<Mutex<HashMap<String, MeshStatus>>>,
    fabric: Arc<Mutex<Option<FabricStatus>>>,
    /// Ticks each time a mesh status is taken or dropped: a routing fact (states.md "Routing"). A
    /// local observation; nothing is sent.
    mesh_changes: Arc<tokio::sync::watch::Sender<u64>>,
}

impl Default for StatusBook {
    fn default() -> Self {
        Self { meshes: Arc::default(), fabric: Arc::default(), mesh_changes: Arc::new(tokio::sync::watch::Sender::new(0)) }
    }
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
                drop(held);
                self.mesh_changes.send_modify(|v| *v += 1);
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

    /// Ticks each time a mesh status is taken or dropped.
    pub fn mesh_changes(&self) -> tokio::sync::watch::Receiver<u64> {
        self.mesh_changes.subscribe()
    }

    /// The status held for `mesh`.
    pub fn mesh(&self, mesh: &str) -> Option<MeshStatus> {
        self.meshes.lock().unwrap().get(mesh).cloned()
    }

    /// Forget the status held for `mesh`: the mesh left, and nothing of it is kept or replayed.
    /// Answers the status that was held. A status dropped is a routing fact as one taken is.
    pub fn forget_mesh(&self, mesh: &str) -> Option<String> {
        let dropped = self.meshes.lock().unwrap().remove(mesh).map(|h| h.status);
        if dropped.is_some() {
            self.mesh_changes.send_modify(|v| *v += 1);
        }
        dropped
    }

    /// Every mesh status held, by mesh name.
    pub fn meshes(&self) -> Vec<(String, MeshStatus)> {
        self.meshes.lock().unwrap().iter().map(|(m, h)| (m.clone(), h.clone())).collect()
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
    /// The contacts a new-fabric-primary announcement gave for the fabric holder's exact birth.
    fabric_contacts: Option<(SeatHolder, FabricContacts)>,
    meshes: HashMap<String, SeatHolder>,
    /// Seat holders' exact births a peer told this node are proven gone (an entry read): the proof
    /// the peer holds, which this node never heard itself.
    gone: std::collections::HashSet<IncarnationId>,
    /// The last seat transfer held together: replayed as one frame while its seats are the records held.
    together: Option<(LifecycleOp, SeatHolder, Vec<CommittedSeat>)>,
    /// The records already durable where they were committed (a transfer's own seat row): whoever
    /// persists the held records writes nothing more for them.
    durable: std::collections::HashSet<(Seat, String, u64)>,
}

/// A fabric-primary's contact addresses as its new-fabric-primary announcement states them: the
/// QUIC UDP address and the HTTP API base bind independently, neither derived from the other.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FabricContacts {
    /// The holder's endpoint key.
    pub endpoint_id: String,
    /// The holder's bound QUIC UDP address.
    pub transport_addr: std::net::SocketAddr,
    /// The holder's control API base URL, when it runs one.
    pub admin_api_base: Option<String>,
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

    /// Hold every seat of one committed transfer together, under one lock: either every seat's
    /// record supersedes the one held and all are held, or none is (the seats a stale transfer
    /// names stay as they are). `durable` says the transfer's own row already holds them, so whoever
    /// persists the held records writes nothing more for them. Returns what happened per seat.
    pub fn take_together(&self, op: &LifecycleOp, successor: &SeatHolder, seats: &[CommittedSeat], durable: bool) -> Vec<SeatTaken> {
        let mut held = self.inner.lock().unwrap();
        let current = |held: &HeldSeats, c: &CommittedSeat| match c.mesh_key() {
            None => held.fabric.clone(),
            Some(mesh) => held.meshes.get(mesh).cloned(),
        };
        let decided: Vec<SeatTaken> = seats
            .iter()
            .map(|c| match current(&held, c) {
                Some(h) if h == c.record() => SeatTaken::Same,
                Some(h) if !c.record().supersedes(&h) => SeatTaken::Refused { held: h },
                previous => SeatTaken::Held { previous },
            })
            .collect();
        if decided.iter().any(|t| matches!(t, SeatTaken::Refused { .. })) {
            return seats.iter().map(|c| SeatTaken::Refused { held: current(&held, c).unwrap_or_else(|| c.record()) }).collect();
        }
        for c in seats {
            let record = c.record();
            match c.mesh_key() {
                None => held.fabric = Some(record.clone()),
                Some(mesh) => {
                    held.meshes.insert(mesh.to_string(), record.clone());
                }
            }
            if durable {
                held.durable.insert((c.seat.kind(), record.mesh.clone(), record.epoch));
            }
        }
        held.together = Some((op.clone(), successor.clone(), seats.to_vec()));
        if decided.iter().any(|t| matches!(t, SeatTaken::Held { .. })) {
            self.changed.send_modify(|v| *v += 1);
        }
        decided
    }

    /// Whether `holder`'s record of `seat` is already durable where it was committed.
    pub fn is_durable(&self, seat: Seat, holder: &SeatHolder) -> bool {
        self.inner.lock().unwrap().durable.contains(&(seat, holder.mesh.clone(), holder.epoch))
    }

    /// The fabric seat's held record.
    pub fn fabric(&self) -> Option<SeatHolder> {
        self.inner.lock().unwrap().fabric.clone()
    }

    /// Attach `contacts` to the held fabric holder when `holder` is that exact birth at that epoch.
    /// `false` when the held record is another one (a delayed announcement attaches to nothing).
    pub fn attach_fabric_contacts(&self, holder: &SeatHolder, contacts: FabricContacts) -> bool {
        let mut held = self.inner.lock().unwrap();
        if held.fabric.as_ref() != Some(holder) {
            return false;
        }
        held.fabric_contacts = Some((holder.clone(), contacts));
        true
    }

    /// The contacts announced for the fabric seat's current holder, when it announced any.
    pub fn fabric_contacts(&self) -> Option<FabricContacts> {
        let held = self.inner.lock().unwrap();
        held.fabric_contacts.as_ref().filter(|(h, _)| held.fabric.as_ref() == Some(h)).map(|(_, c)| c.clone())
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
        // The last transfer, while every seat it moved is still the record held, replays as the one
        // frame: two `Seated` frames would re-open the window between two installs.
        let together = held.together.as_ref().filter(|(_, _, seats)| {
            seats.iter().all(|c| match c.mesh_key() {
                None => held.fabric.as_ref() == Some(&c.record()),
                Some(mesh) => held.meshes.get(mesh) == Some(&c.record()),
            })
        });
        let in_together = |seat: Seat, h: &SeatHolder| together.is_some_and(|(_, _, seats)| seats.iter().any(|c| c.seat.kind() == seat && &c.record() == h));
        let mut out: Vec<Frame> = held.meshes.values().filter(|h| !in_together(Seat::MeshPrimary, h)).map(|h| Frame::Seated { seat: Seat::MeshPrimary, holder: h.clone() }).collect();
        out.sort_by_key(|f| if let Frame::Seated { holder, .. } = f { holder.mesh.clone() } else { String::new() });
        if let Some(h) = held.fabric.as_ref().filter(|h| !in_together(Seat::FabricPrimary, h)) {
            out.push(Frame::Seated { seat: Seat::FabricPrimary, holder: h.clone() });
        }
        if let Some((op, successor, seats)) = together {
            out.push(Frame::SeatedTogether { op: op.clone(), successor: successor.clone(), seats: seats.clone() });
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

/// The mesh a publisher's `path.name` (`<mesh>.<kind>.<ordinal>`) belongs to.
fn publisher_mesh(publisher: &str) -> &str {
    publisher.split('.').next().unwrap_or(publisher)
}

/// What a peer mesh's primary puts on its own mesh channel for a lifecycle or status frame it
/// heard on the backbone: the frame, unchanged but for `forwarded_by`, or nothing. A source Mesh's
/// members are never forwarded as heard: the [`Forwarder`] derives the delta or the stripped full
/// from the two snapshots the primary holds (gossip.md §3.3). `me` is the forwarding node, `own_mesh`
/// its mesh. A status keeps its publisher and its instant: a forwarder never restates one as its
/// own, and forwards only what its author sent (a replay by a holder is for the backbone only).
pub fn forward_of(frame: Frame, me: &str, own_mesh: &str) -> Option<Frame> {
    match frame {
        Frame::NodeDeleting { op, publisher, topology_version, .. } if op.name.mesh != own_mesh => Some(Frame::NodeDeleting { op, forwarded_by: Some(me.to_string()), publisher, topology_version }),
        Frame::NodeDeleted { op, .. } if op.name.mesh != own_mesh => Some(Frame::NodeDeleted { op, forwarded_by: Some(me.to_string()) }),
        Frame::NodeRestarting { op, publisher, topology_version, .. } if op.name.mesh != own_mesh => Some(Frame::NodeRestarting { op, forwarded_by: Some(me.to_string()), publisher, topology_version }),
        Frame::NodeStopped { op, publisher, topology_version, .. } if op.name.mesh != own_mesh => Some(Frame::NodeStopped { op, forwarded_by: Some(me.to_string()), publisher, topology_version }),
        Frame::SeatedTogether { op, successor, seats } if op.name.mesh != own_mesh => Some(Frame::SeatedTogether { op, successor, seats }),
        Frame::NodeDraining { op, .. } if op.name.mesh != own_mesh => Some(Frame::NodeDraining { op, forwarded_by: Some(me.to_string()) }),
        Frame::NodeLeaving { op, .. } if op.name.mesh != own_mesh => Some(Frame::NodeLeaving { op, forwarded_by: Some(me.to_string()) }),
        Frame::NodeLeft { op, .. } if op.name.mesh != own_mesh => Some(Frame::NodeLeft { op, forwarded_by: Some(me.to_string()) }),
        Frame::NodeDrained { op, .. } if op.name.mesh != own_mesh => Some(Frame::NodeDrained { op, forwarded_by: Some(me.to_string()) }),
        Frame::MeshLeaving { mesh_id, build_id, attempt, operation, publisher, event_at_rafka_ms, forwarded_by: None } if publisher_mesh(&publisher) != own_mesh => {
            Some(Frame::MeshLeaving { mesh_id, build_id, attempt, operation, publisher, event_at_rafka_ms, forwarded_by: Some(me.to_string()) })
        }
        Frame::MeshLeave { mesh_id, build_id, attempt, operation, final_node_id, final_incarnation, receipt_manifest, publisher, event_at_rafka_ms, forwarded_by: None } if publisher_mesh(&publisher) != own_mesh => {
            Some(Frame::MeshLeave { mesh_id, build_id, attempt, operation, final_node_id, final_incarnation, receipt_manifest, publisher, event_at_rafka_ms, forwarded_by: Some(me.to_string()) })
        }
        Frame::MeshLeft { mesh_id, build_id, attempt, operation, receipt_manifest, publisher, event_at_rafka_ms, forwarded_by: None } if publisher_mesh(&publisher) != own_mesh => {
            Some(Frame::MeshLeft { mesh_id, build_id, attempt, operation, receipt_manifest, publisher, event_at_rafka_ms, forwarded_by: Some(me.to_string()) })
        }
        Frame::StateCommitting { hook, .. } if hook.publisher_mesh() != own_mesh => Some(Frame::StateCommitting { hook, forwarded_by: Some(me.to_string()) }),
        Frame::StateCommitted { hook, .. } if hook.publisher_mesh() != own_mesh => Some(Frame::StateCommitted { hook, forwarded_by: Some(me.to_string()) }),
        Frame::TrafficOpening { hook, .. } if hook.publisher_mesh() != own_mesh => Some(Frame::TrafficOpening { hook, forwarded_by: Some(me.to_string()) }),
        Frame::TrafficOpened { hook, .. } if hook.publisher_mesh() != own_mesh => Some(Frame::TrafficOpened { hook, forwarded_by: Some(me.to_string()) }),
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
    /// The topic's sender; `None` while the channel is parked (the topic is left).
    sender: Arc<tokio::sync::RwLock<Option<GossipSender>>>,
    /// `true` while the channel is parked: the topic is left and nothing is sent or heard on it.
    parked: Arc<tokio::sync::watch::Sender<bool>>,
    /// Counts the times the receive task has finished leaving the topic for a park.
    left: Arc<tokio::sync::watch::Sender<u64>>,
    /// Every peer this channel was seeded with or asked to join.
    peers: Arc<Mutex<BTreeSet<iroh::EndpointId>>>,
    /// The peers asked while they stayed on the live list.
    joined: Arc<Mutex<BTreeSet<iroh::EndpointId>>>,
    /// The endpoint's peer-location book: shared by every channel and topic of the endpoint.
    book: LocationBook,
    endpoint: Endpoint,
    /// The active gossip neighbours on this channel (NeighborUp minus NeighborDown).
    neighbors: Arc<Mutex<BTreeSet<iroh::EndpointId>>>,
    /// Counts the changes of `neighbors`: what a waiter on "a neighbour is up" wakes on.
    neighbors_changed: Arc<tokio::sync::watch::Sender<u64>>,
    /// Frames broadcast on this channel since this birth.
    frames_sent: Arc<std::sync::atomic::AtomicU64>,
    /// Frames decoded from this channel since this birth.
    frames_received: Arc<std::sync::atomic::AtomicU64>,
}

impl Channel {
    /// Register where the birth `d` names is, and retire the paths a moved key leaves behind.
    fn register(&self, d: &MeshDigest) {
        self.book.register(d);
    }

    async fn join(
        gossip: &Gossip,
        endpoint: &Endpoint,
        topic: TopicId,
        fabric: &str,
        node: &str,
        channel: &str,
        seeds: Vec<EndpointAddr>,
        book: LocationBook,
        on_frame: Arc<dyn Fn(Frame) + Send + Sync>,
        targets: Arc<dyn Fn() -> Held + Send + Sync>,
        replay: Arc<dyn Fn() -> Vec<Frame> + Send + Sync>,
    ) -> Result<Self> {
        for s in &seeds {
            book.fill(s);
        }
        let peers: BTreeSet<iroh::EndpointId> = seeds.iter().map(|s| s.id).collect();
        let sub = gossip.subscribe(topic, peers.iter().copied().collect()).await?;
        tracing::info_span!("rdm.mesh.membership.update.via-subscribe", node, channel, fabric, peers = peers.len())
            .in_scope(|| tracing::info!("subscribed"));
        let (sender, receiver) = sub.split();
        let joined = Arc::new(Mutex::new(peers.clone()));
        let me = Self { sender: Arc::new(tokio::sync::RwLock::new(Some(sender))), parked: Arc::new(tokio::sync::watch::Sender::new(false)), left: Arc::new(tokio::sync::watch::Sender::new(0)), peers: Arc::new(Mutex::new(peers)), joined, book, endpoint: endpoint.clone(), neighbors: Arc::default(), neighbors_changed: Arc::new(tokio::sync::watch::Sender::new(0)), frames_sent: Arc::default(), frames_received: Arc::default() };
        let neighbors = me.neighbors.clone();
        let neighbors_changed = me.neighbors_changed.clone();
        let (sent, received) = (me.frames_sent.clone(), me.frames_received.clone());
        me.refeed(node.to_string(), channel.to_string(), neighbors.clone(), targets);
        let (shared, known, gossip, fabric, channel, node) = (me.sender.clone(), me.peers.clone(), gossip.clone(), fabric.to_string(), channel.to_string(), node.to_string());
        let (parked, left) = (me.parked.clone(), me.left.clone());
        tokio::spawn(async move {
            let mut receiver = Some(receiver);
            loop {
                let mut parked_rx = parked.subscribe();
                let reason = loop {
                    let ev = tokio::select! {
                        ev = receiver.as_mut().expect("the receiver is held while the channel is open").next() => ev,
                        _ = parked_rx.wait_for(|p| *p) => break "parked".to_string(),
                    };
                    match &ev {
                        Some(Ok(Event::Received(m))) => {
                            match Frame::decode(&m.content) {
                                Ok(f) => {
                                    received.fetch_add(1, Ordering::Relaxed);
                                    on_frame(f)
                                }
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
                            neighbors_changed.send_modify(|v| *v += 1);
                            tracing::info_span!("rdm.mesh.connection.update.via-neighbour-up", node = %node, channel = %channel, peer = %p.fmt_short(), neighbours = count)
                                .in_scope(|| tracing::info!("a neighbour came up on this channel"));
                            // A join, a heal or a refeed brings a neighbour up: the holder says the
                            // statuses it holds once (gossip.md §3.1), the original authors intact.
                            let sender = shared.read().await.clone();
                            let frames = if sender.is_some() { replay() } else { Vec::new() };
                            if !frames.is_empty() {
                                let bytes: usize = frames.iter().map(|f| f.encode().len()).sum();
                                let fulls = frames.iter().filter(|f| matches!(f, Frame::Members { .. })).count();
                                tracing::info_span!("rdm.mesh.membership.update.via-neighbour-replay", node = %node, channel = %channel, peer = %p.fmt_short(), frames = frames.len(), full_chunks = fulls, bytes)
                                    .in_scope(|| tracing::info!("a neighbour came up: this primary says what it holds once"));
                            }
                            if let Some(sender) = sender {
                                for f in frames {
                                    if sender.broadcast(Bytes::from(f.encode())).await.is_ok() {
                                        sent.fetch_add(1, Ordering::Relaxed);
                                    }
                                }
                            }
                        }
                        Some(Ok(Event::NeighborDown(p))) => {
                            let count = {
                                let mut n = neighbors.lock().unwrap();
                                n.remove(p);
                                n.len()
                            };
                            neighbors_changed.send_modify(|v| *v += 1);
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
                if reason == "parked" {
                    // The topic is left: the sender and the receiver are both dropped, so this node
                    // is no neighbour of anyone on it. It joins again only when the channel is unparked.
                    *shared.write().await = None;
                    drop(receiver.take());
                    left.send_modify(|n| *n += 1);
                    tracing::info_span!("rdm.mesh.connection.delete.via-channel-parked", node = %node, channel = %channel)
                        .in_scope(|| tracing::info!("the topic is left: the channel is parked"));
                    let _ = parked_rx.wait_for(|p| !*p).await;
                }
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
                *shared.write().await = Some(s);
                receiver = Some(r);
            }
        });
        Ok(me)
    }

    async fn broadcast(&self, f: &Frame) -> Result<()> {
        let sender = self.sender.read().await.clone().ok_or_else(|| anyhow::anyhow!("the channel is parked: its topic is left, nothing is sent"))?;
        sender.broadcast(Bytes::from(f.encode())).await?;
        self.frames_sent.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Leave the topic: the sender and the receiver are dropped, so this node is a neighbour of no
    /// one on it, and nothing is sent or heard until [`Self::unpark`]. Resolves once the topic is
    /// left. Parking a parked channel is a no-op.
    async fn park(&self) {
        if *self.parked.borrow() {
            return;
        }
        let mut left = self.left.subscribe();
        left.borrow_and_update();
        self.parked.send_replace(true);
        let _ = left.changed().await;
    }

    /// Join the topic again through the peers this channel knows.
    async fn unpark(&self) {
        let mut ready = self.sender.read().await.is_some();
        self.parked.send_replace(false);
        while !ready {
            tokio::time::sleep(Duration::from_millis(5)).await;
            ready = self.sender.read().await.is_some();
        }
    }

    /// Register `peer`'s address; `true` when it is new to this channel.
    fn learn(&self, peer: &EndpointAddr) -> bool {
        self.book.fill(peer);
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
            let Some(sender) = self.sender.read().await.clone() else { return Ok(0) };
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
                // its key and never hands it out again. The birth's iroh paths stay open: its
                // typed drain is in flight over them, and they are retired at the proven exit.
                for key in &held.retired {
                    if retired.insert(*key) {
                        me.book.forget(*key);
                        me.peers.lock().unwrap().remove(key);
                        me.joined.lock().unwrap().remove(key);
                        tracing::info_span!("rdm.mesh.connection.update.via-restart-address-retired", node = %node, channel = %channel, peer = %key.fmt_short())
                            .in_scope(|| tracing::info!("a birth under an open restart or removal: its address is dropped from this channel; its paths stay until the exit is proven"));
                    }
                }
                retired.retain(|k| held.retired.contains(k));
                for t in schedule.due(held.targets, Instant::now()) {
                    me.book.fill(&t.addr);
                    me.peers.lock().unwrap().insert(t.addr.id);
                    let sender = me.sender.read().await.clone();
                    let joined = match sender {
                        Some(sender) => sender.join_peers(vec![t.addr.id]).await.is_ok(),
                        None => false,
                    };
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
                let joined = match sender {
                    Some(sender) => sender.join_peers(peers.clone()).await.is_ok(),
                    None => continue,
                };
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
    /// A complete snapshot installed from the backbone: who published it, when, and the
    /// publisher's own digest as the snapshot lists it. Evidence for the clock's discipline.
    publication: Option<Publication>,
}

/// What a receiver held of one source before a projection frame of it applied.
struct Prior {
    held: Option<(PublisherId, u64)>,
    stops: BTreeSet<(String, String)>,
    restarts: BTreeSet<(String, u32, String)>,
}

impl Prior {
    fn of(rx: &SnapshotReceiver, mesh: &str) -> Self {
        Self { held: rx.held_version(mesh), stops: Fence::stops_of(rx.held_full(mesh)), restarts: Fence::restarts_of(rx.held_full(mesh)) }
    }

    /// The fence of a frame of `publisher` at `version` arriving on `side`, `after` the projection the
    /// receiver holds once it applied.
    fn fence(self, publisher: &PublisherId, side: Side, version: u64, after: Option<&Full>) -> Fence {
        Fence {
            publisher: publisher.to_string(),
            channel: side.name(),
            held: self.held.map(|(p, v)| (p.to_string(), v)),
            version,
            prior_stops: self.stops,
            post_stops: Fence::stops_of(after),
            prior_restarts: self.restarts,
            post_restarts: Fence::restarts_of(after),
        }
    }
}

/// A backbone snapshot as its publisher stamped it.
struct Publication {
    publisher: PublisherId,
    /// The publisher's rafka-time when it published (`Frame::Members::published_at_rafka_ms`).
    published_at_rafka_ms: u64,
    /// The publisher's own digest in the snapshot, matched by its exact birth.
    publisher_digest: Option<MeshDigest>,
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
    /// What a node-admin does with a `node-drained` or `node-left` it hears on its mesh channel
    /// from the node itself: carry it onto the backbone when it holds the command (set by the admin).
    completion_carrier: Arc<Mutex<Option<CompletionCarrier>>>,
}

/// What a node-admin does with a node's own completion frame heard on its mesh channel.
pub type CompletionCarrier = Arc<dyn Fn(&Frame) + Send + Sync>;

impl View {
    fn new(mesh: &str, node: &str) -> Self {
        let mut view = Self { mesh: mesh.to_string(), node: node.to_string(), ..Self::default() };
        *view.book.holder.lock().unwrap() = node.to_string();
        *view.book.own_mesh.lock().unwrap() = mesh.to_string();
        // One flag: the view replays as its mesh's primary while the book authors its members' statuses.
        view.primary = view.book.primary.clone();
        view
    }

    /// Hold a status frame if it is newer than the one held: the one door for a status heard on a
    /// channel, replayed or answered by an entry pull. A mesh status whose publisher's birth is held
    /// departed (its mesh's MeshLeft, or its own NodeDeleted), with no live birth holding that name,
    /// arrived after the publisher left and is refused by name: nothing of a mesh that left is held
    /// again or replayed. Every other mesh status heard opens one span saying whether it was taken
    /// (gossip.md: "the held status, until the next change"; a repeat of the change held, or an
    /// older one, is not); one taken is a routing fact on receipt (states.md "Routing"), and the span
    /// names how many of the mesh's held births application routing selects from now on.
    fn take_status(&self, f: &Frame, fabric: &FabricId, via: &'static str) {
        let Frame::MeshStatus { mesh, status, publisher, forwarded_by, changed_at_rafka_ms } = f else {
            self.book.statuses.take(f, fabric);
            return;
        };
        if let Some(op) = self.book.departed().into_iter().find(|op| op.name.to_string() == *publisher) {
            if !self.book.all().iter().any(|d| d.node.name.to_string() == *publisher) {
                reject_departed_overlay(&self.node, &op, "mesh-status");
                return;
            }
        }
        let span = tracing::info_span!(
            "rdm.mesh.membership.update.via-mesh-status",
            node = %self.node,
            mesh = %mesh,
            status = %status,
            publisher = %publisher,
            forwarded_by = forwarded_by.as_deref().unwrap_or(""),
            changed_at_rafka_ms = *changed_at_rafka_ms,
            via,
            taken = tracing::field::Empty,
            births_held = tracing::field::Empty,
            births_routable = tracing::field::Empty,
        );
        let _in = span.enter();
        let taken = self.book.statuses.take(f, fabric);
        span.record("taken", taken);
        if !taken {
            tracing::debug!("a mesh status was heard: a repeat of the change held, or an older one, is not taken");
            return;
        }
        let births: Vec<MeshDigest> = self.book.all().into_iter().filter(|d| &d.node.name.mesh == mesh).collect();
        let routable = births.iter().filter(|d| self.book.routable(d.node.node_id.as_str())).count();
        span.record("births_held", births.len());
        span.record("births_routable", routable);
        tracing::info!("a mesh status is held: application routing selects the mesh's births only while it is ready-for-traffic");
    }

    /// Hold what `f` carries; the digests it carries (for their addresses). A snapshot frame is
    /// not taken here: see [`View::take_snapshot`].
    fn take(&self, f: &Frame, fabric: &FabricId, via: &'static str) -> Vec<MeshDigest> {
        self.take_with(f, fabric, via).0
    }

    /// [`View::take`], and whether a digest the frame carried was newer than the one held: the
    /// book's admission of a heartbeat. A heartbeat taken is answered as the book now holds it, with
    /// the status held for its birth (a heartbeat carries none).
    fn take_with(&self, f: &Frame, fabric: &FabricId, via: &'static str) -> (Vec<MeshDigest>, bool) {
        if let Frame::Digest { digest } = f {
            if &digest.fabric_id == fabric {
                let fresh = self.book.record(digest.clone());
                if fresh {
                    self.note(digest, via);
                }
                let held = fresh.then(|| self.book.get(digest.node.node_id.as_str()).map(|(d, _)| d)).flatten().unwrap_or_else(|| digest.clone());
                return (vec![held], fresh);
            }
        }
        (self.take_other(f, fabric, via), false)
    }

    fn take_other(&self, f: &Frame, fabric: &FabricId, via: &'static str) -> Vec<MeshDigest> {
        match f {
            Frame::NodeDeleting { op, publisher, topology_version, .. } | Frame::NodeRestarting { op, publisher, topology_version, .. } | Frame::NodeStopped { op, publisher, topology_version, .. } => {
                let kind = match f {
                    Frame::NodeDeleting { .. } => StatusEvent::Deleting,
                    Frame::NodeRestarting { .. } => StatusEvent::Restarting,
                    _ => StatusEvent::Stopped,
                };
                if !self.refuses_status_event(kind, op, publisher, *topology_version, via) {
                    self.apply_status_event(kind, op, Some((publisher.to_string(), *topology_version)), via);
                }
                Vec::new()
            }
            Frame::NodeDeleted { op, .. } => {
                self.book.depart(op.clone());
                Vec::new()
            }
            Frame::NodeDraining { op, forwarded_by } | Frame::NodeLeaving { op, forwarded_by } | Frame::NodeLeft { op, forwarded_by } | Frame::NodeDrained { op, forwarded_by } => {
                let kind = match f {
                    Frame::NodeDraining { .. } => "node-draining",
                    Frame::NodeLeaving { .. } => "node-leaving",
                    Frame::NodeDrained { .. } => "node-drained",
                    _ => "node-left",
                };
                tracing::info_span!(
                    "rdm.mesh.membership.update.via-lifecycle-frame",
                    node = %self.node,
                    kind,
                    subject = %op.name,
                    subject_id = %op.node_id,
                    incarnation_id = %op.incarnation.0,
                    build_id = %op.build_id,
                    attempt = op.attempt,
                    operation = %op.operation,
                    forwarded_by = forwarded_by.as_deref().unwrap_or(""),
                    via,
                )
                .in_scope(|| tracing::info!("a lifecycle command or completion frame was heard: evidence of the accepted command, never liveness, completion or exit proof"));
                // A node's own `node-drained` or `node-left`, heard on this mesh's channel: the owning
                // mesh-admin carries it onto the backbone (gossip.md, node-drained, node-left).
                if let (Frame::NodeLeft { .. } | Frame::NodeDrained { .. }, None, "mesh-channel") = (f, forwarded_by, via) {
                    if let Some(carry) = self.completion_carrier.lock().unwrap().clone() {
                        carry(f);
                    }
                }
                Vec::new()
            }
            Frame::MeshLeaving { mesh_id, build_id, attempt, operation, publisher, event_at_rafka_ms, forwarded_by } => {
                // Leave intent, never a mark: a peer learns the mesh's members are leaving from each
                // member's own Draining and Leaving, carried in what it installs (R4).
                tracing::info_span!(
                    "rdm.mesh.membership.update.via-mesh-leaving",
                    node = %self.node,
                    mesh_id = %mesh_id,
                    build_id = %build_id,
                    attempt = *attempt,
                    operation = %operation,
                    publisher = %publisher,
                    forwarded_by = forwarded_by.as_deref().unwrap_or(""),
                    via,
                )
                .in_scope(|| tracing::info!("mesh-leaving heard: leave intent; each member's own Draining and Leaving says it takes no new work"));
                let _ = event_at_rafka_ms;
                Vec::new()
            }
            Frame::MeshLeave { mesh_id, build_id, attempt, operation, final_node_id, receipt_manifest, publisher, forwarded_by, .. } => {
                tracing::info_span!(
                    "rdm.mesh.membership.update.via-mesh-leave",
                    node = %self.node,
                    mesh_id = %mesh_id,
                    build_id = %build_id,
                    attempt = *attempt,
                    operation = %operation,
                    final_node_id = %final_node_id,
                    receipt_manifest = %receipt_manifest,
                    publisher = %publisher,
                    forwarded_by = forwarded_by.as_deref().unwrap_or(""),
                    via,
                )
                .in_scope(|| tracing::info!("the mesh handed over its member receipts: still Leaving, its final primary still running"));
                Vec::new()
            }
            Frame::MeshLeft { mesh_id, build_id, attempt, operation, receipt_manifest, publisher, event_at_rafka_ms, forwarded_by } => {
                let source = self.source_mesh_of(mesh_id);
                let applied = self.book.mesh_left(mesh_id, source.as_deref(), build_id, *attempt, *event_at_rafka_ms);
                // The mesh is no longer part of the topology this node holds: the source projections
                // of it, held or published, go with it, so no replay or top-up carries it again.
                // Its held status goes too (a dead mesh's MeshStatus Dead included): no replay to a
                // joiner carries it after the mesh left, and a mesh created later under the name is
                // a new mesh with a new id (states.md).
                let mut status_dropped = None;
                if let Some(name) = &applied.name {
                    self.forwarder.lock().unwrap().remove(name);
                    self.mesh_rx.lock().unwrap().remove(name);
                    self.backbone_rx.lock().unwrap().remove(name);
                    status_dropped = self.book.statuses.forget_mesh(name);
                }
                tracing::info_span!(
                    "rdm.mesh.membership.update.via-mesh-left",
                    node = %self.node,
                    mesh_id = %mesh_id,
                    build_id = %build_id,
                    attempt = *attempt,
                    operation = %operation,
                    receipt_manifest = %receipt_manifest,
                    publisher = %publisher,
                    forwarded_by = forwarded_by.as_deref().unwrap_or(""),
                    births_departed = applied.departed,
                    mesh_name = applied.name.as_deref().unwrap_or(""),
                    status_dropped = status_dropped.as_deref().unwrap_or(""),
                    via,
                )
                .in_scope(|| tracing::info!("the mesh left: its births departed, its source projections and held status are removed, nothing of it is kept"));
                Vec::new()
            }
            Frame::StateCommitting { hook, forwarded_by } | Frame::StateCommitted { hook, forwarded_by } | Frame::TrafficOpening { hook, forwarded_by } | Frame::TrafficOpened { hook, forwarded_by } => {
                let kind = match f {
                    Frame::StateCommitting { .. } => "state-committing",
                    Frame::StateCommitted { .. } => "state-committed",
                    Frame::TrafficOpening { .. } => "traffic-opening",
                    _ => "traffic-opened",
                };
                tracing::info_span!(
                    "rdm.mesh.membership.update.via-round-frame",
                    node = %self.node,
                    kind,
                    subject_id = %hook.node_id,
                    incarnation_id = %hook.incarnation.0,
                    fabric_id = %hook.fabric_id,
                    build_id = %hook.build_id,
                    attempt = hook.attempt,
                    operation = %hook.operation,
                    publisher = %hook.publisher,
                    forwarded_by = forwarded_by.as_deref().unwrap_or(""),
                    via,
                )
                .in_scope(|| tracing::info!("a fabric round hook was heard: evidence of the accepted command or check-in, never liveness, completion or a lifecycle state"));
                // An ordinary node's check-in, heard on this mesh's channel as its author sent it: the
                // mesh-admin holding the round carries it onto the backbone (gossip.md, "Fabric round gossip hooks").
                if let (Frame::StateCommitted { .. } | Frame::TrafficOpened { .. }, None, "mesh-channel") = (f, forwarded_by, via) {
                    if let Some(carry) = self.completion_carrier.lock().unwrap().clone() {
                        carry(f);
                    }
                }
                Vec::new()
            }
            Frame::MeshStatus { .. } | Frame::FabricStatus { .. } => {
                self.take_status(f, fabric, via);
                Vec::new()
            }
            Frame::Seated { seat, holder } => {
                self.note_seat(*seat, holder, via);
                Vec::new()
            }
            Frame::SeatedTogether { op, successor, seats } => {
                self.note_seats_together(op, successor, seats, via);
                Vec::new()
            }
            Frame::NewFabricPrimary { holder, endpoint_id, transport_addr, admin_api_base } => {
                self.note_new_fabric_primary(holder, endpoint_id, *transport_addr, admin_api_base.as_deref(), via);
                Vec::new()
            }
            Frame::Concern { seat, node_id, incarnation, observer } => {
                self.concerns.put(ConcernHeard { seat: *seat, node_id: node_id.clone(), incarnation: incarnation.clone(), observer: observer.clone() });
                Vec::new()
            }
            _ => Vec::new(),
        }
    }

    /// Whether the status event `kind` of `op`'s birth, published by `publisher` at
    /// `topology_version` and heard through `via`, is refused (node-status-heartbeat-connections.md
    /// §C "status before seat evidence": "a status fact from a publisher the receiver does not yet
    /// hold as seated is refused by name (nothing is held for later); the top-up supplies the full").
    /// The publisher is seated when the held seat record of the subject's mesh names its exact birth,
    /// or it is the held fabric-primary (the acting primary of a mesh with no primary, R-NS1). A
    /// publisher held stopped or departed publishes nothing, so its event was delayed. A
    /// `NodeStopped` older than a held projection of the same publisher that lists the exact birth
    /// without the stop is refused too (§C "late stopped after starting"): a receiver orders a fact
    /// only against facts of the same publisher epoch.
    fn refuses_status_event(&self, kind: StatusEvent, op: &LifecycleOp, publisher: &PublisherId, topology_version: u64, via: &'static str) -> bool {
        let mesh = op.name.mesh.as_str();
        let side = if via == "mesh-channel" { Side::MeshChannel } else { Side::Backbone };
        let unseated = if let Some(reason) = self.book.publisher_left(&publisher.incarnation) {
            Some((reason, None))
        } else if self.book.seats.fabric().is_some_and(|f| f.incarnation == publisher.incarnation) {
            None
        } else {
            match self.book.seats.mesh(mesh) {
                Some(h) if h.incarnation == publisher.incarnation => None,
                Some(h) => Some(("not-seated", Some(h))),
                None => Some(("no-seat-record", None)),
            }
        };
        if let Some((reason, seat)) = unseated {
            // On the mesh channel, a seat record naming another birth is the evidence an unknown
            // source epoch is (gossip.md §3.1): the source is marked for GetTopology repair, exactly as
            // a projection of that publisher is ([`View::refuse_predecessor`]). With no seat record the
            // node has not read its topology yet: that read is its top-up, and supplies the full.
            let repair = if side == Side::MeshChannel && reason == "not-seated" {
                self.mesh_rx.lock().unwrap().unknown_epoch(mesh, publisher);
                self.desync.notify_one();
                "get-topology"
            } else {
                "none"
            };
            tracing::info_span!(
                "rdm.mesh.membership.reject.via-unseated-publisher",
                node = %self.node,
                frame = kind.name(),
                subject = %op.name,
                subject_id = %op.node_id,
                incarnation_id = %op.incarnation.0,
                build_id = %op.build_id,
                attempt = op.attempt,
                operation = %op.operation,
                publisher = %publisher,
                topology_version,
                channel = side.name(),
                reason,
                seat_holder = %seat.map(|s| s.to_string()).unwrap_or_default(),
                fabric_primary = %self.book.seats.fabric().map(|s| s.to_string()).unwrap_or_default(),
                repair,
            )
            .in_scope(|| tracing::info!("a status event from a publisher this node does not hold seated for the subject's mesh: refused, nothing is held for later"));
            return true;
        }
        if kind != StatusEvent::Stopped {
            return false;
        }
        let key = (op.node_id.to_string(), op.incarnation.0.clone());
        for (channel, rx) in [("mesh-channel", &self.mesh_rx), ("backbone", &self.backbone_rx)] {
            let rx = rx.lock().unwrap();
            let Some((held_publisher, held_version)) = rx.held_version(mesh) else { continue };
            if held_publisher != *publisher || held_version < topology_version {
                continue;
            }
            let Some(full) = rx.held_full(mesh) else { continue };
            let listed = full.digests().iter().any(|d| d.node.node_id == op.node_id && d.node.incarnation == op.incarnation);
            if !listed || Fence::stops_of(Some(full)).contains(&key) {
                continue;
            }
            drop(rx);
            tracing::info_span!(
                "rdm.mesh.membership.reject.via-stop-older-than-projection",
                node = %self.node,
                subject = %op.name,
                subject_id = %op.node_id,
                incarnation_id = %op.incarnation.0,
                build_id = %op.build_id,
                attempt = op.attempt,
                operation = %op.operation,
                publisher = %publisher,
                topology_version,
                held_version,
                held_channel = channel,
                via,
            )
            .in_scope(|| tracing::info!("a NodeStopped older than the held projection of its publisher, which lists the birth without the stop: refused, the birth started after it"));
            return true;
        }
        false
    }

    /// Apply the status event `kind` of `op`'s birth to the book: the frame a seated publisher sent
    /// (`carried`: its publisher and the version at which the subject changed), or the event the
    /// executor itself publishes (`carried: None`; it authors the projection).
    fn apply_status_event(&self, kind: StatusEvent, op: &LifecycleOp, carried: Option<(String, u64)>, via: &str) -> bool {
        match kind {
            StatusEvent::Deleting => self.book.deleting(op.clone()),
            StatusEvent::Restarting => self.book.restarting_carried(op.clone(), carried),
            StatusEvent::Stopped => {
                let prior = self.book.get(op.node_id.as_str()).map(|(h, _)| h.status);
                let taken = self.book.stopped_carried(op.clone(), carried);
                if taken {
                    self.note_member_parked(op, prior, via);
                }
                taken
            }
        }
    }

    /// Why a projection frame of `mesh` from `publisher`, arriving on `side` while this node holds
    /// the source from `held`, is a predecessor's publication (node-status-heartbeat-connections.md
    /// §A.3: "a delayed predecessor publication cannot switch receivers back"); `None` when it is
    /// not. Author ordering is the publisher epochs and the LeadershipView (§A.3; gossip.md §3.3:
    /// "the source publisher's exact identity is its epoch"):
    /// - the publisher's birth is held stopped or departed here ([`DigestBook::publisher_left`]): it
    ///   publishes nothing, so the frame was delayed, on every side;
    /// - on the backbone or the mesh channel, a frame of another epoch than the one held, from a
    ///   publisher the held seat record of `mesh` does not name and that is not the held
    ///   fabric-primary (the acting primary of a mesh with no primary, R-NS1): `not-seated`, naming
    ///   the seat holder. On the mesh channel that is an unknown source epoch, which marks the source
    ///   for GetTopology repair ([`View::refuse_predecessor`]). A receiver that holds no seat record of
    ///   `mesh` has no evidence against it.
    ///   A topology read is this node's own top-up from the primary it chose by that record, so a
    ///   read is refused only for a publisher that left.
    fn predecessor(&self, mesh: &str, publisher: &PublisherId, held: Option<&PublisherId>, side: Side) -> Option<(&'static str, Option<SeatHolder>)> {
        if let Some(reason) = self.book.publisher_left(&publisher.incarnation) {
            return Some((reason, None));
        }
        if !matches!(side, Side::Backbone | Side::MeshChannel) || held == Some(publisher) {
            return None;
        }
        let holder = self.book.seats.mesh(mesh)?;
        let acting = self.book.seats.fabric().is_some_and(|f| f.incarnation == publisher.incarnation);
        (holder.incarnation != publisher.incarnation && !acting).then_some(("not-seated", Some(holder)))
    }

    /// A predecessor's publication refused by name: what it was, who published it, what this node
    /// holds of the source, and why the publisher is not the epoch this node takes.
    #[allow(clippy::too_many_arguments)]
    fn refuse_predecessor(&self, side: Side, frame: &'static str, mesh: &str, publisher: &PublisherId, topology_version: u64, held: Option<&(PublisherId, u64)>, reason: &'static str, seat: Option<&SeatHolder>) {
        // On the mesh channel, a publisher without seat evidence is an unknown source epoch: the
        // node cannot tell a delayed predecessor from a successor whose seat no frame of its channel
        // carried (a peer mesh's `Seated` never reaches it), so its own primary's topology repairs
        // the source (gossip.md §3.1: "an unknown source epoch trigger[s] GetTopology repair").
        let repair = if side == Side::MeshChannel && reason == "not-seated" {
            self.mesh_rx.lock().unwrap().unknown_epoch(mesh, publisher);
            self.desync.notify_one();
            "get-topology"
        } else {
            "none"
        };
        tracing::info_span!(
            "rdm.mesh.membership.reject.via-predecessor-publisher",
            node = %self.node,
            channel = side.name(),
            frame,
            mesh = %mesh,
            publisher = %publisher,
            topology_version,
            held_publisher = %held.map(|(p, _)| p.to_string()).unwrap_or_default(),
            held_version = held.map(|(_, v)| *v as i64).unwrap_or(-1),
            reason,
            seat_holder = %seat.map(ToString::to_string).unwrap_or_default(),
            repair,
        )
        .in_scope(|| tracing::info!("a publication of an epoch this node does not take: refused, the held projection stands"));
    }

    /// A `Members` chunk or a `MembersDelta`, arrived on `side`. A snapshot is applied to the book
    /// only once complete; a delta only at exactly its base. A frame that does not belong on its
    /// channel, and a predecessor's publication ([`View::predecessor`]), are refused by name.
    fn take_snapshot(&self, f: &Frame, fabric: &FabricId, side: Side) -> SnapshotTaken {
        match f {
            Frame::Members { mesh, publisher, forwarded_by, topology_version, published_at_rafka_ms, snapshot_id, chunk_index, chunk_count, digests, in_flight, departed } => {
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
                if self.outside_build(mesh, side) {
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
                let held = rx.held_version(mesh);
                if let Some((reason, seat)) = self.predecessor(mesh, publisher, held.as_ref().map(|(p, _)| p), side) {
                    drop(rx);
                    self.refuse_predecessor(side, "members", mesh, publisher, *topology_version, held.as_ref(), reason, seat.as_ref());
                    return SnapshotTaken::default();
                }
                let prior = Prior::of(&rx, mesh);
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
                        let installed = self.installed(&i, fabric, side, *published_at_rafka_ms, prior);
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
                if self.outside_build(mesh, side) {
                    return SnapshotTaken::default();
                }
                let delta = Delta { changed: changed.clone(), removed: removed.clone(), in_flight: in_flight.clone(), departed: departed.clone() };
                let mut rx = self.mesh_rx.lock().unwrap();
                let held = rx.held_version(mesh);
                if let Some((reason, seat)) = self.predecessor(mesh, source_publisher, held.as_ref().map(|(p, _)| p), side) {
                    drop(rx);
                    self.refuse_predecessor(side, "members-delta", mesh, source_publisher, *topology_version, held.as_ref(), reason, seat.as_ref());
                    return SnapshotTaken::default();
                }
                let prior = Prior::of(&rx, mesh);
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
                        let fence = prior.fence(source_publisher, side, *topology_version, rx.held_full(mesh));
                        let carried = self.apply_full(mesh, &full, fabric, side, &fence, source_publisher.node.split('.').next() == Some(mesh.as_str()));
                        drop(rx);
                        SnapshotTaken { carried, ..SnapshotTaken::default() }
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

    /// Whether a snapshot or delta of the peer mesh `mesh` installs nothing: the accepted Build is
    /// the authority on which meshes the fabric has, and a mesh it does not name, of which this
    /// node holds no source projection, is not a mesh this node takes members of from an old
    /// aggregate. A source this node holds is a mesh it still tracks (a Leaving mesh) and is
    /// updated; a node that holds no Build takes everything.
    fn outside_build(&self, mesh: &str, side: Side) -> bool {
        if mesh == self.mesh || self.book.accepted_has_mesh(mesh) != Some(false) {
            return false;
        }
        if self.receiver(side).lock().unwrap().held_version(mesh).is_some() {
            return false;
        }
        tracing::info_span!("rdm.mesh.membership.reject.via-mesh-not-in-build", node = %self.node, channel = side.name(), mesh)
            .in_scope(|| tracing::info!("a topology of a mesh the accepted Build does not name, of which this node holds no source: installs nothing"));
        true
    }

    /// The name of the mesh `mesh_id` names in a source projection this node holds (received on
    /// the backbone or the mesh channel, or forwarded): a node-admin's digest in it states the id.
    fn source_mesh_of(&self, mesh_id: &MeshId) -> Option<String> {
        let names = |full: &Full| full.digests().into_iter().find(|d| d.mesh_id.as_ref() == Some(mesh_id)).map(|d| d.node.name.mesh.clone());
        let received = [&self.backbone_rx, &self.mesh_rx].into_iter().find_map(|rx| rx.lock().unwrap().sources().into_iter().find_map(|(_, _, _, full)| names(&full)));
        received.or_else(|| self.forwarder.lock().unwrap().snapshots().into_iter().find_map(|s| names(&s.full())))
    }

    fn receiver(&self, side: Side) -> &Arc<Mutex<SnapshotReceiver>> {
        match side {
            Side::Backbone => &self.backbone_rx,
            Side::MeshChannel | Side::Read | Side::ReadPeer => &self.mesh_rx,
        }
    }

    /// A complete snapshot is held: apply it to the book. `prior` is what the receiver held of the
    /// source before it.
    fn installed(&self, i: &Install, fabric: &FabricId, side: Side, published_at_rafka_ms: u64, prior: Prior) -> SnapshotTaken {
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
        // The publisher is a node of the mesh it publishes (its primary), or of another mesh: the
        // fabric primary as the acting primary of a mesh with no primary. Only the first hears the
        // members it publishes.
        let publishers_own = i.publisher.node.split('.').next() == Some(i.mesh.as_str());
        let fence = prior.fence(&i.publisher, side, i.topology_version, Some(&i.full));
        let carried = self.apply_full(&i.mesh, &i.full, fabric, side, &fence, publishers_own);
        let source = (side == Side::Backbone && i.mesh != self.mesh).then(|| (i.mesh.clone(), i.publisher.clone(), i.topology_version, i.full.clone()));
        // The fabric primary's clock is sampled from its own mesh's aggregate, never from one it
        // publishes as the acting primary of a mesh with no primary: that publisher is a node of
        // another mesh.
        let publication = (side == Side::Backbone && publishers_own).then(|| Publication {
            publisher: i.publisher.clone(),
            published_at_rafka_ms,
            publisher_digest: i.full.digests().into_iter().find(|d| d.node.incarnation == i.publisher.incarnation),
        });
        SnapshotTaken { carried, source, publication }
    }

    /// Apply the members, overlays and departures of a snapshot or a delta to the book. Departures
    /// first: a digest of a departed birth in the same snapshot is refused. A listed birth this book
    /// holds stopped ends its stop only when the source moved, in order, from a version carrying the
    /// stop to this one (`fence`); any other entry of it is refused by name. An own-mesh birth's
    /// entry gives its status and nothing else (gossip.md §3.1: liveness, endpoints and the runtime
    /// fact come from its Digest), unless this node is the primary that authors it. `publishers_own`:
    /// the publisher is a node of the mesh it publishes, its primary, which carries only members it
    /// hears; a peer mesh's births in an acting primary's aggregate (the fabric primary for a mesh
    /// with no primary, a node of another mesh) are topology, never liveness.
    fn apply_full(&self, mesh: &str, full: &Full, fabric: &FabricId, side: Side, fence: &Fence, publishers_own: bool) -> Vec<MeshDigest> {
        for op in full.departed() {
            self.book.depart(op);
        }
        for op in full.in_flight() {
            if op.is_restart() {
                self.book.restarting(op);
            } else if op.is_stop() {
                let prior = self.book.get(op.node_id.as_str()).map(|(h, _)| h.status);
                if self.book.stopped(op.clone()) {
                    self.note_member_parked(&op, prior, side.name());
                }
            } else {
                self.book.deleting(op);
            }
        }
        self.book.note_stops_carried(fence);
        self.book.note_restarts_carried(fence);
        let authors = self.primary.load(Ordering::Relaxed);
        // The primary ends its own mesh's restarts at node.started; every other node when its
        // projection moves past one.
        if !(mesh == self.mesh && authors) {
            self.book.restarts_moved_past(mesh, fence);
        }
        let mut carried = Vec::new();
        for d in full.digests().into_iter().filter(|d| &d.fabric_id == fabric) {
            // A read carries this node's own digest as the node read holds it: this node's own
            // book is the authority on itself.
            if matches!(side, Side::Read | Side::ReadPeer) && d.node.name.to_string() == self.node {
                continue;
            }
            let own = d.node.name.mesh == self.mesh;
            // The primary authors its own mesh's statuses: a projection of its own mesh, on its own
            // channel or on the backbone (a predecessor's last aggregate after this node took the
            // seat), moves none of them.
            if own && authors && matches!(side, Side::MeshChannel | Side::Backbone) {
                continue;
            }
            if !(own && authors) && self.book.stopped_entry(&d, fence) == Some(false) {
                continue;
            }
            let prior = self.book.get(d.node.node_id.as_str()).map(|(h, _)| h.status);
            let taken = match (side, own) {
                // An own-mesh member's status from its primary's projection, or its acting primary's
                // (gossip.md §3.1 `Members` author): nothing else of it, so no liveness from either.
                (Side::Backbone, true) | (Side::MeshChannel, true) => {
                    self.book.take_projected_status(&d, fence);
                    continue;
                }
                (Side::Backbone, false) if publishers_own => self.book.record_forwarded(d.clone()),
                // The acting primary of a mesh with no primary (the fabric primary, a node of another
                // mesh) hears none of that mesh's members: what it carries of them is topology, and
                // their liveness comes from their own word or their own primary's aggregate alone.
                (Side::Backbone, false) => self.book.record_topology(d.clone()),
                (Side::MeshChannel, false) => self.book.record_topology(d.clone()),
                // A read installs what an entry answer installed, by the rule `learn` applies
                // (R-G2): a peer mesh's member is topology and refreshes no liveness; a member of
                // this node's own mesh is held as the answerer heard it, with its status.
                (Side::Read, true) => self.book.record_forwarded(d.clone()),
                (Side::Read, false) | (Side::ReadPeer, false) => self.book.record_topology(d.clone()),
                (Side::ReadPeer, true) => false,
            };
            if taken {
                self.note(&d, side.via());
                self.note_member_state(&d, prior, side);
                carried.push(d);
            }
        }
        carried
    }

    /// A peer mesh's member whose own state this node now holds changed (or was first held saying
    /// Draining or Leaving): what this node routes to it follows that state alone (R4).
    fn note_member_state(&self, d: &MeshDigest, prior: Option<rafka_mesh_entity::MemberStatus>, side: Side) {
        use rafka_mesh_entity::MemberStatus::{Draining, Leaving};
        if d.node.name.mesh == self.mesh || prior == Some(d.status) || (prior.is_none() && !matches!(d.status, Draining | Leaving)) {
            return;
        }
        tracing::info_span!(
            "rdm.mesh.membership.update.via-member-state",
            node = %self.node,
            member = %d.node.name,
            member_id = %d.node.node_id,
            incarnation_id = %d.node.incarnation.0,
            status = ?d.status,
            prior = prior.map(|p| format!("{p:?}")).unwrap_or_default(),
            channel = side.name(),
            routable = self.book.routable(d.node.node_id.as_str()),
        )
        .in_scope(|| tracing::info!("a peer mesh's member state held from what this node installed: routing follows it"));
    }

    /// A peer mesh's member this node now holds parked, from its mesh primary's `NodeStopped` overlay
    /// (gossip.md, node-stopped lifecycle overlay): Leaving and parked, not routable. `channel` is
    /// where the overlay arrived: the frame itself, or the aggregate's `in_flight`.
    fn note_member_parked(&self, op: &rafka_mesh_entity::LifecycleOp, prior: Option<rafka_mesh_entity::MemberStatus>, channel: &str) {
        if op.name.mesh == self.mesh {
            return;
        }
        tracing::info_span!(
            "rdm.mesh.membership.update.via-member-state",
            node = %self.node,
            member = %op.name,
            member_id = %op.node_id,
            incarnation_id = %op.incarnation.0,
            status = ?rafka_mesh_entity::MemberStatus::Leaving,
            prior = prior.map(|p| format!("{p:?}")).unwrap_or_default(),
            channel,
            routable = self.book.routable(op.node_id.as_str()),
            parked = true,
            operation = %op.operation,
            build_id = %op.build_id,
        )
        .in_scope(|| tracing::info!("a peer mesh's member is parked by its mesh primary's NodeStopped: Leaving, not routable"));
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

    /// Hold the seats of a committed transfer together, naming what it did with them.
    fn note_seats_together(&self, op: &LifecycleOp, successor: &SeatHolder, seats: &[CommittedSeat], via: &'static str) {
        let taken = self.book.seats.take_together(op, successor, seats, false);
        if taken.iter().all(|t| matches!(t, SeatTaken::Same)) {
            return;
        }
        let outcome = if taken.iter().any(|t| matches!(t, SeatTaken::Refused { .. })) { "refused-not-newer" } else { "held" };
        let named: Vec<String> = seats.iter().map(|c| format!("{}={}", c.seat.name(), c.record())).collect();
        tracing::info_span!("rdm.mesh.seat.update.via-seated-together", node = %self.node, operation = %op.operation, build_id = %op.build_id, attempt = op.attempt, former = %op.name, successor = %successor, seats = %named.join(","), via, outcome)
            .in_scope(|| tracing::info!("a committed seat transfer was heard"));
    }

    /// Hold the fabric seat record a new-fabric-primary announcement carries under the seat's own
    /// acceptance rule (a stale holder is refused by name), and attach its contacts to that exact
    /// holder birth unless this book holds a digest of the birth, which is the contact authority.
    fn note_new_fabric_primary(&self, holder: &SeatHolder, endpoint_id: &str, transport_addr: std::net::SocketAddr, admin_api_base: Option<&str>, via: &'static str) {
        let taken = self.book.seats.take(Seat::FabricPrimary, holder);
        let outcome = match &taken {
            SeatTaken::Held { .. } => "held",
            SeatTaken::Same => "same",
            SeatTaken::Refused { .. } => "refused-not-newer",
        };
        let has_digest = self.book.get(holder.node_id.as_str()).is_some_and(|(d, _)| d.node.incarnation == holder.incarnation);
        let attached = !has_digest && !matches!(taken, SeatTaken::Refused { .. }) && self.book.seats.attach_fabric_contacts(holder, FabricContacts { endpoint_id: endpoint_id.to_string(), transport_addr, admin_api_base: admin_api_base.map(String::from) });
        tracing::info_span!(
            "rdm.mesh.seat.update.via-new-fabric-primary",
            node = %self.node,
            holder = %holder,
            endpoint_id,
            transport_addr = %transport_addr,
            admin_api_base = admin_api_base.unwrap_or(""),
            outcome,
            previous = %match &taken { SeatTaken::Held { previous } => previous.as_ref().map(ToString::to_string).unwrap_or_default(), SeatTaken::Refused { held } => held.to_string(), SeatTaken::Same => String::new() },
            contacts_attached = attached,
            digest_held = has_digest,
            via,
        )
        .in_scope(|| tracing::info!("the committed fabric-primary announced itself"));
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
    /// What the forwarder decides and where it goes: this mesh's channel.
    forward_path: ForwardPath,
    /// The config this birth's mesh-primary last told it (`ApplyNodeConfig`; ops-naming "Node data"):
    /// carried in every digest this membership publishes from then on. `None` until it is told.
    config: Arc<Mutex<Option<std::collections::BTreeMap<String, String>>>>,
    /// The name the mesh-primary's row last gave this birth (`ApplyNodeName`, R12): carried in every
    /// digest this membership publishes from then on. `None` while the birth holds its launch name.
    name: Arc<Mutex<Option<rafka_mesh_entity::PathName>>>,
}

impl Membership {
    /// The process's iroh endpoint this membership rides: for read-only observations
    /// (`crate::iroh_obs`), never for sending.
    pub fn endpoint(&self) -> Endpoint {
        self.mesh.endpoint.clone()
    }

    /// The endpoint's peer-location book: every other topic this process joins on the endpoint
    /// installs its peers' addresses through it.
    pub fn location_book(&self) -> LocationBook {
        self.mesh.book.clone()
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
                let mut frames = replay_view.book.statuses.held_frames(&replay_fabric, &replay_me);
                frames.extend(replay_view.book.seats.held_frames());
                frames.extend(replay_view.forwarder.lock().unwrap().replay(&replay_me, replay_clock.now_rafka_ms()));
                frames
            } else {
                Vec::new()
            }
        });
        let location = LocationBook::new(endpoint, node)?;
        let (v, f, ls, observing) = (view.clone(), fabric.clone(), location.clone(), clock.clone());
        let on_frame: Arc<dyn Fn(Frame) + Send + Sync> = Arc::new(move |frame: Frame| {
            let carried = match &frame {
                Frame::Members { .. } | Frame::MembersDelta { .. } => v.take_snapshot(&frame, &f, Side::MeshChannel).carried,
                _ => {
                    let (carried, fresh) = v.take_with(&frame, &f, "mesh-channel");
                    // A member's clock follows its current mesh primary's heartbeat, judged on the
                    // digest as its book holds it: the primary's status is its own word, in its
                    // projection, never on its heartbeat.
                    if let (Frame::Digest { .. }, Some(held)) = (&frame, carried.first()) {
                        let verdict = crate::discipline::from_mesh_primary_digest(&v.mesh, &v.node, v.primary.load(Ordering::Relaxed), &v.book.seats, held, fresh);
                        crate::discipline::offer(&observing, &v.node, "mesh-primary-digest", verdict);
                    }
                    carried
                }
            };
            for d in &carried {
                ls.register(d);
            }
        });
        let (book, own_mesh, me_name) = (view.book.clone(), mesh.to_string(), node.to_string());
        let targets: Arc<dyn Fn() -> Held + Send + Sync> =
            Arc::new(move || held_view(&book, |d| d.node.name.mesh == own_mesh && d.node.name.to_string() != me_name));
        let channel = Channel::join(gossip, endpoint, mesh_topic(fabric, mesh_id), fabric.as_str(), node, &format!("mesh:{mesh}"), seeds, location, on_frame, targets, replay).await?;
        let sink_channel = channel.clone();
        let sink: crate::forward_path::Sink = Arc::new(move |f: Frame| {
            let channel = sink_channel.clone();
            Box::pin(async move { channel.broadcast(&f).await })
        });
        let forward_path = ForwardPath::new(view.forwarder.clone(), Arc::default(), sink);
        let me = Self { mesh: channel, book: view.book.clone(), view, fabric: fabric.clone(), cut_off: Arc::default(), clock, digest_seq: Arc::default(), load: Arc::new(crate::load::LoadSampler::for_this_process()), forward_path, config: Arc::default(), name: Arc::default() };
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

    /// Hold the digest a birth presented at its join (`JoinNode`), and register where it is. A join
    /// digest carries no status: an own-mesh member is held as heard, with the status this book holds
    /// for it ([`DigestBook::record_joined`]); a peer mesh's member is topology, its held status kept.
    pub fn learn(&self, d: MeshDigest, via: &'static str) {
        if d.fabric_id != self.fabric {
            return;
        }
        self.mesh.register(&d);
        let taken = if d.node.name.mesh == self.view.mesh {
            self.book.record_joined(d.clone())
        } else {
            let held = self.book.get(d.node.node_id.as_str()).filter(|(h, _)| h.node.incarnation == d.node.incarnation).map(|(h, _)| h.status);
            self.book.record_topology(MeshDigest { status: held.unwrap_or(rafka_mesh_entity::MemberStatus::Pending), ..d.clone() })
        };
        if taken {
            self.view.note(&d, via);
        }
    }

    /// This node's path name, as it publishes itself.
    pub fn node(&self) -> &str {
        &self.view.node
    }

    /// Hold (or drop) this node as its mesh's primary: while it is, it authors its own mesh's member
    /// statuses from what its members report to it and replays what it holds to a neighbour coming
    /// up; otherwise it takes them from its primary's projection (gossip.md §3.1 `Members`).
    /// [`Backbone::set_mesh_primary`] sets it with the seat; a launcher that executes its mesh's
    /// node lifecycle with no backbone sets it here.
    pub fn set_mesh_primary(&self, primary: bool) {
        self.view.primary.store(primary, Ordering::Relaxed);
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
        self.view.book.statuses.fabric()
    }

    /// The status of `mesh` this node holds: the last one published, until the next change.
    pub fn mesh_status(&self, mesh: &str) -> Option<MeshStatus> {
        self.view.book.statuses.mesh(mesh)
    }

    /// Every mesh status this node holds, by mesh name: each the last one published for its mesh.
    pub fn mesh_statuses(&self) -> Vec<(String, MeshStatus)> {
        self.view.book.statuses.meshes()
    }

    /// The statuses this node holds, as the frames it replays (original publisher and instant
    /// kept): what an entry pull answers (`entry::EntryAnswer::statuses`).
    pub fn status_frames(&self, me: &str) -> Vec<Frame> {
        self.view.book.statuses.held_frames(&self.fabric, me)
    }

    /// Hold the statuses an entry pull answered.
    pub fn learn_statuses(&self, frames: &[Frame]) {
        for f in frames {
            self.view.take_status(f, &self.fabric, "entry-answer");
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
    /// sequence (from 1), `emitted_at_rafka_ms` is the composed clock's reading, and `node_state` is
    /// the liveness it self-publishes. The order a receiver acts on is the sequence, whatever the
    /// clock reads.
    pub async fn publish(&self, d: &MeshDigest) -> Result<()> {
        *self.book.own.lock().unwrap() = Some(d.node.node_id.to_string());
        let mut d = self.with_config(d);
        d.digest_seq = self.digest_seq.fetch_add(1, Ordering::SeqCst) + 1;
        d.emitted_at_rafka_ms = self.clock.now_rafka_ms();
        // The liveness this birth self-publishes (node-identity-and-state.md §4), from its own word
        // about itself; its status never travels on its heartbeat.
        d.node_state = rafka_mesh_entity::NodeState::self_published(d.status);
        d.load = Some(self.load.sample());
        d.gossip = Some(rafka_mesh_entity::GossipStats {
            heard: self.book.current(self.book.staleness_floor()).len() as u32,
            neighbours: self.mesh.neighbors.lock().unwrap().len() as u32,
            frames_sent: self.mesh.frames_sent.load(Ordering::Relaxed),
            frames_received: self.mesh.frames_received.load(Ordering::Relaxed),
        });
        self.book.record(d.clone());
        self.mesh.broadcast(&Frame::Digest { digest: d }).await
    }

    /// Hold `config` as this birth's config: every digest this membership publishes from now on carries
    /// it (ops-naming "Node data": the node applies it and carries it in its next digest).
    pub fn set_config(&self, config: std::collections::BTreeMap<String, String>) {
        *self.config.lock().unwrap() = Some(config);
    }

    /// Hold `name` as this birth's name: every digest this membership publishes from now on carries
    /// it (R12: the node publishes only the name its row holds).
    pub fn set_name(&self, name: rafka_mesh_entity::PathName) {
        *self.name.lock().unwrap() = Some(name);
    }

    /// The name this birth publishes: the one it was told ([`Self::set_name`]), else `birth`'s.
    pub fn held_name(&self, birth: &rafka_mesh_entity::PathName) -> rafka_mesh_entity::PathName {
        self.name.lock().unwrap().clone().unwrap_or_else(|| birth.clone())
    }

    /// `d` as this birth reports it: with the config and the name it was told, when it was told them.
    pub fn with_config(&self, d: &MeshDigest) -> MeshDigest {
        let mut d = d.clone();
        if let Some(c) = self.config.lock().unwrap().as_ref() {
            d.config = c.clone();
        }
        if let Some(n) = self.name.lock().unwrap().as_ref() {
            d.node.name = n.clone();
        }
        d
    }

    /// Install what this node-admin does with a node's own `node-drained` / `node-left` heard on
    /// the mesh channel.
    pub fn set_completion_carrier(&self, carrier: CompletionCarrier) {
        *self.view.completion_carrier.lock().unwrap() = Some(carrier);
    }

    /// Broadcast a frame onto this mesh's channel (a forward).
    pub async fn forward(&self, f: &Frame) -> Result<()> {
        self.mesh.broadcast(f).await
    }

    /// Leave this mesh's gossip topic: this node is a neighbour of no one on it and sends and hears
    /// nothing there until [`Membership::unpark`]. Resolves once the topic is left.
    pub async fn park(&self) {
        self.mesh.park().await;
    }

    /// Join this mesh's gossip topic again through the peers it knows. Resolves once the topic is
    /// re-subscribed.
    pub async fn unpark(&self) {
        self.mesh.unpark().await;
    }

    /// Resolves once this node's mesh channel has a gossip neighbour (at once when it has one). A node
    /// that rejoins waits here before it says it is back: what it says before it has a neighbour
    /// reaches no one.
    pub async fn wait_for_neighbour(&self) {
        let mut changed = self.mesh.neighbors_changed.subscribe();
        loop {
            changed.borrow_and_update();
            if !self.mesh.neighbors.lock().unwrap().is_empty() {
                return;
            }
            if changed.changed().await.is_err() {
                return;
            }
        }
    }

    /// The active gossip neighbours on this node's mesh channel.
    pub fn neighbours(&self) -> usize {
        self.mesh.neighbors.lock().unwrap().len()
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
        // A mesh's primary that has not published yet still holds its members in its book: it serves
        // them as its own source at version 0 (the version before any publication, the one its
        // publisher reports), never as nothing. Its first publication supersedes it.
        if self.view.primary.load(Ordering::Relaxed) && held.iter().all(|s| &s.mesh != mesh) {
            held.push(SourceSnapshot {
                mesh: mesh.clone(),
                publisher: PublisherId { node: own.node.name.to_string(), incarnation: own.node.incarnation.clone() },
                topology_version: 0,
                digests: Vec::new(),
                in_flight: Vec::new(),
                departed: Vec::new(),
            });
        }
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
        let side = if answerer_mesh == self.view.mesh { Side::Read } else { Side::ReadPeer };
        let mut rx = self.view.mesh_rx.lock().unwrap();
        let held = rx.held_version(&chunk.mesh);
        if let Some((reason, seat)) = self.view.predecessor(&chunk.mesh, &chunk.publisher, held.as_ref().map(|(p, _)| p), side) {
            drop(rx);
            self.view.refuse_predecessor(side, "read", &chunk.mesh, &chunk.publisher, chunk.topology_version, held.as_ref(), reason, seat.as_ref());
            return Taken::Refused(crate::snapshot::Refusal::PredecessorPublisher { reason });
        }
        let prior = Prior::of(&rx, &chunk.mesh);
        let taken = rx.take_read_chunk(chunk);
        if let Taken::Installed(i) = &taken {
            self.view.installed(i, &self.fabric, side, 0, prior);
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

    /// This node's own Mesh's current primary node-admin: the birth its held seat record names
    /// ([`DigestBook::mesh_primary`]). The node tops up from it and from no other.
    pub fn mesh_primary(&self) -> Result<MeshDigest, NoMeshPrimary> {
        self.book.mesh_primary(&self.view.mesh)
    }

    /// Resume desynchronized sources from this Mesh's own primary (gossip.md §3.3): when a delta
    /// did not follow what is held, the node reads the current topology of each desynchronized
    /// mesh from its own mesh-primary (Node RPC op `0x1E`), installs it atomically, and resumes.
    /// Never the remote Mesh, never over gossip. One attempt per gap signal: nothing is retried or
    /// held for later; the next delta or full from the primary signals or resumes it again.
    pub fn spawn_top_up(&self, fetch: TopUpFetch) -> tokio::task::JoinHandle<()> {
        tokio::spawn(self.top_ups(fetch))
    }

    /// The top-up of [`Self::spawn_top_up`] as a task for the caller to run: a node-admin runs it
    /// behind its park gate, so a parked admin tops up nothing and a gap signalled while it was parked
    /// is topped up once it runs again.
    pub fn top_ups(&self, fetch: TopUpFetch) -> impl std::future::Future<Output = ()> + Send + 'static {
        let me = self.clone();
        async move {
            loop {
                // One attempt per signal: a delta heard on a desynchronized source signals again,
                // so a top-up that failed is attempted when the primary next speaks, never in a loop.
                me.view.desync.notified().await;
                let gaps = me.desynced_sources();
                if !gaps.is_empty() {
                    me.top_up(&fetch, gaps).await;
                }
            }
        }
    }

    async fn top_up(&self, fetch: &TopUpFetch, gaps: Vec<(String, Gap)>) {
        let meshes = gaps.iter().map(|(m, _)| m.as_str()).collect::<Vec<_>>().join(",");
        let reasons = gaps.iter().map(|(m, g)| format!("{m}:{g}")).collect::<Vec<_>>().join(",");
        let node = self.view.node.clone();
        let primary = match self.mesh_primary() {
            Ok(primary) => primary,
            Err(no) => {
                tracing::info_span!("rdm.mesh.entry.reject.via-top-up", node = %node, meshes = %meshes, reasons = %reasons, reason = no.reason(), seat_holder = %no.holder().map(|h| h.to_string()).unwrap_or_default(), detail = %no)
                    .in_scope(|| tracing::info!("this Mesh's current primary cannot be addressed: nothing to top up from"));
                return;
            }
        };
        // The Mesh's primary has no primary to top up from: it serves its own mesh from what it
        // publishes and every peer mesh from the backbone, never from its mesh-channel sources.
        if self.book.own.lock().unwrap().as_deref() == Some(primary.node.node_id.as_str()) {
            tracing::info_span!("rdm.mesh.entry.reject.via-top-up", node = %node, meshes = %meshes, reasons = %reasons, mesh_primary = %primary.node.name, reason = "this-node-is-the-mesh-primary")
                .in_scope(|| tracing::info!("this node is its Mesh's primary: it has no primary to top up from, and it serves no source from its mesh channel"));
            return;
        }
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

/// Why a node cannot address its Mesh's current primary: each a distinct fact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NoMeshPrimary {
    /// The node holds no seat record of the mesh.
    NoSeatRecord {
        /// The mesh whose record is not held.
        mesh: String,
    },
    /// The record names a birth whose digest the node does not hold.
    HolderNotHeld {
        /// The record held.
        holder: SeatHolder,
        /// The birth of the same node the node holds instead, when it holds one.
        held_incarnation: Option<IncarnationId>,
        /// Whether the node holds the holder's node as departed.
        departed: bool,
    },
    /// The record's birth is held as proven gone.
    HolderGone {
        /// The record held.
        holder: SeatHolder,
    },
}

impl NoMeshPrimary {
    /// The span reason naming this fact.
    pub fn reason(&self) -> &'static str {
        match self {
            Self::NoSeatRecord { .. } => "no-seat-record-held",
            Self::HolderNotHeld { .. } => "seat-holder-not-held",
            Self::HolderGone { .. } => "seat-holder-gone",
        }
    }

    /// The seat record held, when one is.
    pub fn holder(&self) -> Option<&SeatHolder> {
        match self {
            Self::NoSeatRecord { .. } => None,
            Self::HolderNotHeld { holder, .. } | Self::HolderGone { holder } => Some(holder),
        }
    }
}

impl std::fmt::Display for NoMeshPrimary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoSeatRecord { mesh } => write!(f, "no seat record of mesh {mesh} is held"),
            Self::HolderNotHeld { holder, held_incarnation: Some(other), .. } => write!(f, "the seat record names {holder}; the node holds birth {} of that node instead", other.0),
            Self::HolderNotHeld { holder, held_incarnation: None, departed } => write!(f, "the seat record names {holder}; no digest of that node is held{}", if *departed { " (held departed)" } else { "" }),
            Self::HolderGone { holder } => write!(f, "the seat record names {holder}, a birth held as proven gone"),
        }
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
pub type TopUpFetch = std::sync::Arc<dyn Fn(Membership, MeshDigest, Vec<String>) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<TopUpDone, String>> + Send>> + Send + Sync>;

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
    /// The version counter of each mesh this admin published as its publisher of record (R-NS1).
    acting: Arc<Mutex<BTreeMap<String, SourceVersion>>>,
    /// The stops and deletes this admin executed for each mesh beyond its own, as its last round
    /// looked at them: the round is due at once when they move.
    acting_seen: Arc<Mutex<BTreeMap<String, (Vec<LifecycleOp>, Vec<LifecycleOp>)>>>,
}

impl Backbone {
    /// Leave the backbone topic: this admin is a neighbour of no one on it and sends and hears
    /// nothing there until [`Backbone::unpark`]. Resolves once the topic is left.
    pub async fn park(&self) {
        self.channel.park().await;
    }

    /// Join the backbone topic again through the peers it knows.
    pub async fn unpark(&self) {
        self.channel.unpark().await;
    }

    /// Join the backbone as `node`, whose current birth is `incarnation`: the exact identity every
    /// aggregate this admin publishes names as its publisher.
    pub async fn join(gossip: &Gossip, endpoint: &Endpoint, membership: &Membership, mesh: &str, node: &str, incarnation: IncarnationId, seeds: Vec<EndpointAddr>) -> Result<Self> {
        let (m, me, own) = (membership.clone(), node.to_string(), mesh.to_string());
        let on_frame: Arc<dyn Fn(Frame) + Send + Sync> = Arc::new(move |frame: Frame| {
            if matches!(frame, Frame::Members { .. } | Frame::MembersDelta { .. }) {
                let taken = m.view.take_snapshot(&frame, &m.fabric, Side::Backbone);
                // A mesh primary's clock follows its current fabric-primary's aggregate.
                if let Some(p) = &taken.publication {
                    let verdict = crate::discipline::from_fabric_primary_members(&m.view.node, m.view.primary.load(Ordering::Relaxed), &m.view.book.seats, &p.publisher, p.publisher_digest.as_ref(), p.published_at_rafka_ms);
                    crate::discipline::offer(&m.clock, &m.view.node, "fabric-primary-members", verdict);
                }
                for d in &taken.carried {
                    m.mesh.register(d);
                }
                // A complete snapshot of a source Mesh: the forwarding primary says what moved
                // since it last published that source into its own Mesh. The forward path decides
                // nothing unless this node holds the forwarder role, read under its lock.
                if let Some((source_mesh, publisher, version, full)) = taken.source {
                    m.forward_path.source(&me, &source_mesh, &publisher, version, &full, m.clock.now_rafka_ms());
                }
                return;
            }
            for d in m.view.take(&frame, &m.fabric, "backbone") {
                m.mesh.register(&d);
            }
            if let Some(f) = forward_of(frame, &me, &own) {
                m.forward_path.carry(&me, f);
            }
        });
        let (replay_view, replay_fabric, replay_me) = (membership.view.clone(), membership.fabric.clone(), node.to_string());
        let replay: Arc<dyn Fn() -> Vec<Frame> + Send + Sync> = Arc::new(move || {
            if replay_view.primary.load(Ordering::Relaxed) {
                let mut frames = replay_view.book.statuses.held_frames(&replay_fabric, &replay_me);
                frames.extend(replay_view.book.seats.held_frames());
                frames
            } else {
                Vec::new()
            }
        });
        let (book, own_mesh) = (membership.book.clone(), mesh.to_string());
        let targets: Arc<dyn Fn() -> Held + Send + Sync> =
            Arc::new(move || held_view(&book, |d| d.node.name.mesh != own_mesh && d.node.name.kind == rafka_mesh_entity::NodeKind::NodeAdmin));
        let channel = Channel::join(gossip, endpoint, backbone_topic(&membership.fabric), membership.fabric.as_str(), node, "backbone", seeds, membership.mesh.book.clone(), on_frame, targets, replay).await?;
        let me = Self {
            channel,
            node: node.to_string(),
            mesh: mesh.to_string(),
            fabric: membership.fabric.clone(),
            publishing: Arc::default(),
            status_publishing: Arc::default(),
            membership: membership.clone(),
            mesh_status: Arc::new(Mutex::new(StatusPublisher::new(node, StatusScope::Mesh(mesh.to_string())))),
            fabric_status: Arc::new(Mutex::new(StatusPublisher::new(node, StatusScope::Fabric(membership.fabric.clone())))),
            wake: Arc::default(),
            publisher: PublisherId { node: node.to_string(), incarnation },
            full_due: membership.forward_path.owes_full(),
            version: Arc::default(),
            own: Arc::default(),
            acting: Arc::default(),
            acting_seen: Arc::default(),
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
            let heard_mesh = view.book.statuses.mesh(&self.mesh).map(|h| StatusFact { status: h.status, changed_at_rafka_ms: h.changed_at_rafka_ms });
            let heard_fabric = view.book.statuses.fabric().map(|h| StatusFact { status: h.status, changed_at_rafka_ms: h.changed_at_rafka_ms });
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
        self.membership.set_mesh_primary(primary);
        // A forwarder that takes (or loses) the seat has published nothing into its Mesh: the
        // first publication of every source after taking it is a full (gossip.md §3.3). The role,
        // its baseline and its term switch together under the forwarder's lock.
        let forwarder_changed = self.membership.forward_path.set_role(primary);
        self.mesh_status.lock().unwrap().set_role(primary);
        if let Some(role) = Self::role(&self.publishing, primary) {
            tracing::info_span!("rdm.mesh.backbone.update.via-aggregate-publisher", node = %self.node, mesh = %self.mesh, role)
                .in_scope(|| tracing::info!("mesh aggregate publication"));
        }
        if forwarder_changed {
            let role = if primary { "start" } else { "stop" };
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
    /// Returns whether it published: `false` when this node does not hold the publisher role, and
    /// nothing was sent.
    pub async fn publish(&self, membership: &Membership, members: Vec<MeshDigest>) -> bool {
        let sent = membership.clock.now_rafka_ms();
        if !self.publishing.load(Ordering::Relaxed) {
            return false;
        }
        // The projection is read and versioned under one hold of the version: a status event stamped
        // in between ([`Backbone::publish_status_event`]) is either wholly before it or wholly after.
        let (full, version, snapshot_id, bumped) = {
            let mut v = self.version.lock().unwrap();
            let full = self.own_full(membership, members);
            let before = v.version();
            let version = v.observe(&full);
            (full, version, v.next_snapshot_id(), version != before)
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
        // The primary's own member projection goes onto its own mesh channel through the same
        // full and delta machinery, under its epoch and this version (gossip.md §3.1, `Members` and
        // `MembersDelta`): own-mesh members take every member's status from it, and liveness,
        // endpoints and the runtime fact from each member's own Digest. Loads are omitted.
        *self.own.lock().unwrap() = Some((version, full.clone()));
        if self.full_due.swap(false, Ordering::Relaxed) {
            self.forward_fulls().await;
        } else {
            self.membership.forward_path.source(&self.node, &self.mesh, &self.publisher, version, &full, sent);
        }
        true
    }

    /// While this admin is the fabric primary it is the acting primary of a mesh with no primary
    /// (gossip.md §3) whose births it stopped or deleted. For each such mesh it publishes the mesh's
    /// own Members aggregate on the backbone, publisher itself: `digests` as held (gossip.md §3.1),
    /// the members of the aggregate it holds of that mesh from the backbone but the births it stopped
    /// or deleted; the stops it executed in `in_flight` (each from the matching stopped reply it
    /// holds) and the deletes in `departed` (each from the exit proof it holds). It puts the same
    /// source into its own Mesh. A living mesh keeps its members (a recovering admin that reads this
    /// topology finds a local node of its own mesh, mesh-recovery.md steps 3-4); R-NS1's dead mesh,
    /// whose last aggregate held only its last admin, which this admin stopped, has no digests. A
    /// birth it did not stop or delete is never listed in `in_flight` or `departed`. A start ends the
    /// stop and a departure past retention lists nothing: then nothing is published for the mesh.
    /// Returns the meshes published.
    pub async fn publish_for_primaryless(&self, membership: &Membership, primary_of: impl Fn(&str) -> Option<String>) -> Vec<String> {
        let executed = if self.status_publishing.load(Ordering::Relaxed) { membership.book.executed_overlays_beyond(&self.mesh) } else { BTreeMap::new() };
        let seen = std::mem::replace(&mut *self.acting_seen.lock().unwrap(), executed.clone());
        let mut published = Vec::new();
        for (mesh, (in_flight, departed)) in executed {
            if let Some(primary) = primary_of(&mesh) {
                // The round ran at once on the move (round_wake); a mesh whose seat a primary still
                // holds is not primaryless, so nothing is published for it. Said once per move.
                if seen.get(&mesh) != Some(&(in_flight.clone(), departed.clone())) {
                    tracing::info_span!("rdm.mesh.backbone.reject.via-mesh-has-primary", node = %self.node, mesh = %mesh, primary = %primary, in_flight = in_flight.len(), departed = departed.len())
                        .in_scope(|| tracing::info!("a stop or delete this fabric primary executed moved, and the mesh's seat is held: it is not a mesh with no primary"));
                }
                continue;
            }
            let sent = membership.clock.now_rafka_ms();
            let full = Self::acting_full(membership, &mesh, in_flight, departed);
            let (version, snapshot_id, bumped) = {
                let mut acting = self.acting.lock().unwrap();
                let v = acting.entry(mesh.clone()).or_default();
                let before = v.version();
                let version = v.observe(&full);
                (version, v.next_snapshot_id(), version != before)
            };
            if bumped {
                tracing::info_span!("rdm.mesh.backbone.update.via-acting-primary", node = %self.node, mesh = %mesh, publisher = %self.publisher, topology_version = version, digests = full.member_count(), in_flight = full.in_flight().len(), departed = full.departed().len())
                    .in_scope(|| tracing::info!("a mesh with no primary: the fabric primary publishes its aggregate, the members it holds of the mesh and the births it stopped or deleted"));
            }
            for frame in crate::snapshot::chunks_of(&full, |digests, in_flight, departed, chunk_index, chunk_count| Frame::Members {
                mesh: mesh.clone(),
                publisher: self.publisher.clone(),
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
            membership.forward_path.source(&self.node, &mesh, &self.publisher, version, &full, sent);
            published.push(mesh);
        }
        published
    }

    /// The aggregate this fabric primary publishes as the acting primary of `mesh`, a mesh with no
    /// primary: the members held of the mesh, but the births this admin stopped or deleted: those are
    /// in the aggregate's own `in_flight` and `departed`, never among its digests. The acting round
    /// and the version a status event it executed is stamped with read the same aggregate.
    fn acting_full(membership: &Membership, mesh: &str, in_flight: Vec<LifecycleOp>, departed: Vec<LifecycleOp>) -> Full {
        let gone: BTreeSet<(String, String)> = in_flight.iter().chain(departed.iter()).map(|op| (op.node_id.to_string(), op.incarnation.0.clone())).collect();
        let held: Vec<MeshDigest> = membership.view.backbone_rx.lock().unwrap().held_full(mesh).map(Full::digests).unwrap_or_default();
        let digests: Vec<MeshDigest> = held.into_iter().filter(|d| !gone.contains(&(d.node.node_id.to_string(), d.node.incarnation.0.clone()))).collect();
        Full::new(digests, in_flight, departed)
    }

    /// This Mesh's members as a publication round puts them into its aggregate: every member of
    /// this Mesh the book heard within the staleness floor.
    pub fn own_members(&self, membership: &Membership) -> Vec<MeshDigest> {
        membership.book.current(membership.book.staleness_floor()).into_iter().filter(|d| d.node.name.mesh == self.mesh).collect()
    }

    /// This Mesh's aggregate as a round publishes it: `members` with this mesh's own overlays and
    /// departures only (a peer mesh's travel in that mesh's aggregate).
    fn own_full(&self, membership: &Membership, members: Vec<MeshDigest>) -> Full {
        let (in_flight, departed) = membership.book.overlays_of(&self.mesh);
        Full::new(members, in_flight, departed)
    }

    /// Resolves when the next publication round is due: at `deadline`, the anti-entropy timer, or
    /// at once while this admin is its Mesh's publisher and the Mesh's normalized projection is no
    /// longer the one its `topology_version` was last bumped on (gossip.md, `Members`: "at once on a
    /// material change"). The projection is looked at when the book says something it carries
    /// moved (a member's birth or status, a member heard again, an overlay or a departure), never
    /// on a timer.
    pub async fn round_wake(&self, membership: &Membership, deadline: tokio::time::Instant) {
        let sleep = tokio::time::sleep_until(deadline);
        tokio::pin!(sleep);
        let book = &membership.book;
        let (mut heard, mut births, mut overlays) = (book.heard_changes(), book.birth_changes(), book.overlay_changes());
        loop {
            heard.borrow_and_update();
            births.borrow_and_update();
            overlays.borrow_and_update();
            // A stop or delete this fabric primary executed beyond its own mesh moved since its last
            // round looked: the round decides at once whether it publishes for that mesh (R-NS1).
            if self.status_publishing.load(Ordering::Relaxed) && book.executed_overlays_beyond(&self.mesh) != *self.acting_seen.lock().unwrap() {
                return;
            }
            if self.publishing.load(Ordering::Relaxed) {
                let full = self.own_full(membership, self.own_members(membership));
                let published = self.own.lock().unwrap().as_ref().map(|(v, _)| *v).unwrap_or(0);
                let (moved, held) = {
                    let v = self.version.lock().unwrap();
                    // A status event stamped a version no round has published yet: the projection
                    // carrying it goes out at once.
                    (v.would_bump(&full) || published != v.version(), v.version())
                };
                if moved {
                    tracing::info_span!("rdm.mesh.backbone.update.via-material-change", node = %self.node, mesh = %self.mesh, held_topology_version = held, members = full.member_count(), in_flight = full.in_flight().len(), departed = full.departed().len())
                        .in_scope(|| tracing::info!("this mesh's normalized projection moved since its last publication: the round publishes at once"));
                    return;
                }
            }
            let ok = tokio::select! {
                _ = &mut sleep => return,
                r = heard.changed() => r.is_ok(),
                r = births.changed() => r.is_ok(),
                r = overlays.changed() => r.is_ok(),
            };
            if !ok {
                (&mut sleep).await;
                return;
            }
        }
    }

    /// The forwarded full of every source this primary holds (its own Mesh and each peer Mesh),
    /// without loads, on taking the seat: the first publication of every source into this Mesh.
    /// Nothing sends it again on a timer; a held source whose peer Mesh is unheard is not
    /// re-sent, so no member's coverage is renewed by a word its primary did not receive.
    pub async fn forward_fulls(&self) {
        let mut held: Vec<(String, PublisherId, u64, Full)> = self.membership.view.backbone_rx.lock().unwrap().sources().into_iter().filter(|(m, ..)| m != &self.mesh).collect();
        if let Some((version, full)) = self.own.lock().unwrap().clone() {
            held.push((self.mesh.clone(), self.publisher.clone(), version, full));
        }
        let now = self.membership.clock.now_rafka_ms();
        self.membership.forward_path.fulls(&self.node, &self.mesh, "full-due", &held, now);
    }

    /// Publish the status event `kind` of `op`'s birth, which this admin executed (R-HB1 "The version
    /// field": "Each status fact carries its subject's `topology_version`"; §A.2: "A status fact
    /// carries the `topology_version` at which its subject changed"). The event is applied to this
    /// admin's own book, then stamped with its publisher and the version at which the subject changed,
    /// then broadcast on its own mesh channel only (the backbone carries it as the overlay of its
    /// mesh's aggregate):
    /// - this mesh's primary, for its own mesh: the version its projection takes with the event
    ///   applied, observed and versioned under one hold, so its next round publishes exactly that
    ///   version, carrying the event;
    /// - the fabric-primary as the acting primary of a mesh with no primary (R-NS1): the version of
    ///   that mesh's aggregate over the stops and deletes it executed;
    /// - otherwise: this admin's own publisher at the version it holds, and the span names the role.
    ///
    /// Returns the publisher and version stamped.
    pub async fn publish_status_event(&self, kind: StatusEvent, op: &LifecycleOp) -> (PublisherId, u64) {
        let membership = &self.membership;
        let (role, version) = if op.name.mesh == self.mesh && self.publishing.load(Ordering::Relaxed) {
            let mut v = self.version.lock().unwrap();
            membership.view.apply_status_event(kind, op, None, "executed");
            if kind == StatusEvent::Stopped {
                membership.book.executed_stop(op);
            }
            let full = self.own_full(membership, self.own_members(membership));
            ("mesh-primary", v.observe(&full))
        } else if op.name.mesh != self.mesh && self.status_publishing.load(Ordering::Relaxed) {
            let mut acting = self.acting.lock().unwrap();
            membership.view.apply_status_event(kind, op, None, "executed");
            if kind == StatusEvent::Stopped {
                membership.book.executed_stop(op);
            }
            let (in_flight, departed) = membership.book.executed_overlays_beyond(&self.mesh).remove(&op.name.mesh).unwrap_or_default();
            ("acting-fabric-primary", acting.entry(op.name.mesh.clone()).or_default().observe(&Self::acting_full(membership, &op.name.mesh, in_flight, departed)))
        } else {
            membership.view.apply_status_event(kind, op, None, "executed");
            if kind == StatusEvent::Stopped {
                membership.book.executed_stop(op);
            }
            ("executor", self.version.lock().unwrap().version())
        };
        tracing::info_span!(
            "rdm.mesh.membership.update.via-status-event",
            node = %self.node,
            frame = kind.name(),
            subject = %op.name,
            subject_id = %op.node_id,
            incarnation_id = %op.incarnation.0,
            build_id = %op.build_id,
            attempt = op.attempt,
            operation = %op.operation,
            publisher = %self.publisher,
            topology_version = version,
            role,
        )
        .in_scope(|| tracing::info!("a status event this admin executed: applied to its own book, stamped with the version at which its subject changed, broadcast on its mesh channel"));
        if let Err(e) = membership.mesh.broadcast(&kind.frame(op.clone(), self.publisher.clone(), version)).await {
            tracing::info!(error = %e, frame = kind.name(), "the status event was not sent on the mesh channel");
        }
        (self.publisher.clone(), version)
    }

    /// Publish a hook (`node-draining`, `node-leaving`, or a fabric round hook) this admin authored: the backbone,
    /// its own mesh channel and its own view, once. The accepted command's hook, never a separately
    /// coordinated publication. An error names each channel the frame was not handed to, and why.
    pub async fn publish_command_hook(&self, f: &Frame) -> Result<()> {
        let backbone = self.channel.broadcast(f).await;
        let mesh = self.membership.forward(f).await;
        self.membership.view.take(f, &self.fabric, "mesh-channel");
        let failed: Vec<String> = [("backbone", backbone), ("mesh channel", mesh)].into_iter().filter_map(|(channel, r)| r.err().map(|e| format!("{channel}: {e}"))).collect();
        if failed.is_empty() {
            Ok(())
        } else {
            Err(anyhow::anyhow!("{} not handed to {}", self.node, failed.join("; ")))
        }
    }

    /// Carry a node's own completion frame (`node-drained`, `node-left`) onto the backbone,
    /// preserving `op` and naming this admin in `forwarded_by`.
    pub async fn carry_completion(&self, frame: Frame) {
        let carried = match frame {
            Frame::NodeLeft { op, .. } => Frame::NodeLeft { op, forwarded_by: Some(self.node.clone()) },
            Frame::NodeDrained { op, .. } => Frame::NodeDrained { op, forwarded_by: Some(self.node.clone()) },
            Frame::StateCommitted { hook, .. } => Frame::StateCommitted { hook, forwarded_by: Some(self.node.clone()) },
            Frame::TrafficOpened { hook, .. } => Frame::TrafficOpened { hook, forwarded_by: Some(self.node.clone()) },
            _ => return,
        };
        let _ = self.channel.broadcast(&carried).await;
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

    /// The seat holder that committed a Build-driven seat transfer says so once, from its row: the
    /// backbone, its own mesh channel and its own view, before it answers SeatsCommitted.
    pub async fn announce_seats_together(&self, op: LifecycleOp, successor: SeatHolder, seats: Vec<CommittedSeat>) {
        let named: Vec<String> = seats.iter().map(|c| format!("{}={}", c.seat.name(), c.record())).collect();
        let span = tracing::info_span!("rdm.mesh.seat.update.via-announce-seated-together", node = %self.node, operation = %op.operation, build_id = %op.build_id, attempt = op.attempt, successor = %successor, seats = %named.join(","), backbone = tracing::field::Empty, mesh_channel = tracing::field::Empty);
        let frame = Frame::SeatedTogether { op, successor, seats };
        let send = async {
            // Each send's outcome is recorded where it happens: `sent` is the frame handed to that
            // channel's gossip topic (Plumtree delivers it from there), else the channel's error.
            let outcome = |channel: &'static str, r: Result<()>| match r {
                Ok(()) => "sent".to_string(),
                Err(e) => {
                    tracing::info!(channel, error = %e, "SeatedTogether not sent on the {channel}");
                    format!("failed: {e}")
                }
            };
            let backbone = outcome("backbone", self.channel.broadcast(&frame).await);
            tracing::Span::current().record("backbone", backbone.as_str());
            let mesh_channel = outcome("mesh channel", self.membership.forward(&frame).await);
            tracing::Span::current().record("mesh_channel", mesh_channel.as_str());
            let _ = self.membership.view.take(&frame, &self.fabric, "own");
        };
        tracing::Instrument::instrument(send, span).await
    }

    /// The committed fabric-primary announces itself and its contacts on the backbone, once, after
    /// its seat record is committed locally. Nothing repeats it: a joiner is replayed the seat
    /// record by a primary, like any seat.
    pub async fn announce_new_fabric_primary(&self, holder: SeatHolder, endpoint_id: String, transport_addr: std::net::SocketAddr, admin_api_base: Option<String>) {
        let span = tracing::info_span!("rdm.mesh.seat.update.via-announce-new-fabric-primary", node = %self.node, holder = %holder, endpoint_id = %endpoint_id, transport_addr = %transport_addr, admin_api_base = admin_api_base.as_deref().unwrap_or(""));
        let frame = Frame::NewFabricPrimary { holder, endpoint_id, transport_addr, admin_api_base };
        let _ = tracing::Instrument::instrument(self.channel.broadcast(&frame), span).await;
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

/// Whether a forwarded digest carries its status: a member projection entry (a topology read) does;
/// a join digest, like every heartbeat, does not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Carried {
    Status,
    NoStatus,
}

/// How a held member was last heard. Only `Direct` and `Forwarded` are liveness: a word received
/// on the mesh channel or the backbone. `Topology` is a member known only from forwarded topology
/// (a delta, a full, a replay, an entry answer): it is held, and never ages to silent for want of
/// a forwarded frame (gossip.md §3.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Heard {
    /// The member's own digest, received here.
    Direct,
    /// A peer mesh's member listed in an aggregate received on the backbone: it ages from that
    /// receipt at the staleness floor, as a member heard directly does (R-G2: a node-admin judges a
    /// peer mesh from direct backbone receipt).
    Forwarded,
    /// Known only from topology forwarded onto the mesh channel.
    Topology,
}

/// What a book holds of one stopped birth (`NodeStopped`).
#[derive(Debug, Clone)]
struct StopMark {
    /// The stopped birth.
    incarnation: IncarnationId,
    /// The `digest_seq` of the digest this book held of it when it dropped it, 0 when it held none.
    held_seq: u64,
    /// The digest this book held of it when it dropped it: a node-admin keeps showing the birth.
    held: Option<MeshDigest>,
    /// The latest digest of the stopped birth this book refused since, with when it was heard and
    /// how: what the mesh-primary holds the birth from again at `node.joined`, and what a member
    /// holds an own-mesh birth from again when its projection ends the stop.
    latest: Option<(MeshDigest, Instant, Heard)>,
    /// The highest version, and its publisher, at which a projection of the birth's mesh this book
    /// applied carried the stop. A later projection of that source listing the birth moved past it.
    carried: Option<(String, u64)>,
}

/// One projection frame as its receiver applies it: who published it, on which channel, and what
/// the receiver held of that source before and after it. With the version at which a projection
/// carried a stop ([`StopMark::carried`]), what tells a projection that moved past a stop from one
/// that never carried it here.
#[derive(Debug, Clone, Default)]
pub(crate) struct Fence {
    /// The source's publisher (its exact birth): the version's epoch.
    publisher: String,
    /// Where the frame arrived.
    channel: &'static str,
    /// The source held before the frame, `(publisher, version)`, when one was held.
    held: Option<(String, u64)>,
    /// The version the frame carries.
    version: u64,
    /// The stopped births `(node id, incarnation)` whose stop the held source carried before the frame.
    prior_stops: BTreeSet<(String, String)>,
    /// The stopped births whose stop the held source carries after the frame.
    post_stops: BTreeSet<(String, String)>,
    /// The restarts, by operation key, the held source carried before the frame.
    prior_restarts: BTreeSet<(String, u32, String)>,
    /// The restarts the held source carries after the frame.
    post_restarts: BTreeSet<(String, u32, String)>,
}

impl Fence {
    /// The stop births a source projection carries.
    fn stops_of(full: Option<&crate::snapshot::Full>) -> BTreeSet<(String, String)> {
        full.map(|f| f.in_flight().into_iter().filter(LifecycleOp::is_stop).map(|o| (o.node_id.to_string(), o.incarnation.0)).collect()).unwrap_or_default()
    }

    /// The restarts, by operation key, a source projection carries.
    fn restarts_of(full: Option<&crate::snapshot::Full>) -> BTreeSet<(String, u32, String)> {
        full.map(|f| f.in_flight().into_iter().filter(LifecycleOp::is_restart).map(|o| o.key()).collect()).unwrap_or_default()
    }

    /// The version held before the frame, for spans: -1 when none was held.
    fn held_version(&self) -> i64 {
        self.held.as_ref().map(|(_, v)| *v as i64).unwrap_or(-1)
    }
}

/// The latest digest held per logical node: when it was taken, and how it was heard.
#[derive(Debug, Clone)]
pub struct DigestBook {
    inner: Arc<Mutex<HashMap<String, (MeshDigest, Instant, Heard)>>>,
    /// Ticks when a member's birth (`MeshDigest::node`) is first held or
    /// changes, or a departure is accepted; a fresher digest of the same
    /// birth does not tick.
    births: Arc<tokio::sync::watch::Sender<u64>>,
    /// Ticks when a held member is first held, changes its birth or its status, or is heard again
    /// after it had gone silent past the staleness floor: what a local waiter on "that node is
    /// ready" or "that node is reachable again" wakes on. A local observation of the book; nothing
    /// is sent.
    heard: Arc<tokio::sync::watch::Sender<u64>>,
    /// Ticks when an open lifecycle overlay is added or cleared, or a departure is accepted: the
    /// overlays and departures a mesh primary's aggregate carries. A local observation; nothing is
    /// sent.
    overlays: Arc<tokio::sync::watch::Sender<u64>>,
    /// Ticks each time a digest is taken, a fresher digest of a held birth included (its name,
    /// config, load or `digest_seq` moved): what a local waiter on "that node published again"
    /// wakes on. A local observation of the book; nothing is sent.
    digests: Arc<tokio::sync::watch::Sender<u64>>,
    /// Proven departures by NodeId, with when this book accepted each: held
    /// for `retention` of local age, then forgotten.
    departed: Arc<Mutex<HashMap<String, (LifecycleOp, Instant)>>>,
    /// The departures the departed fence refused, by the NodeId of the departure held: each operation
    /// key refused by name once. Its publisher carries it in every aggregate for the retention, and a
    /// later copy is a copy of a refusal already made. Forgotten with the departure it was refused by.
    refused_departures: Arc<Mutex<HashMap<String, BTreeSet<(String, u32, String)>>>>,
    /// Open lifecycle overlays by operation key: the node is held, and not routable.
    in_flight: Arc<Mutex<HashMap<(String, u32, String), LifecycleOp>>>,
    /// The stopped births (`NodeStopped`) by NodeId: what this book held of each when it dropped it,
    /// and the latest heartbeat of it refused since. No heartbeat of the stopped birth is taken; only
    /// the mesh-primary's `node.joined` ([`Self::joined`]), its projection moving past the stop
    /// (`View::apply_full`), a later incarnation of the NodeId or a departure ends the stop.
    stopped_marks: Arc<Mutex<HashMap<String, StopMark>>>,
    /// The open restarts by operation key, each with the highest version, and its publisher, at which
    /// a `NodeRestarting` this book took, or a projection of its mesh it applied, carried it: a later
    /// projection of that source no longer carrying it moved past it (its mesh-primary took
    /// node.started).
    restarts_carried: Arc<Mutex<HashMap<(String, u32, String), (String, u64)>>>,
    /// Whether this book's holder is its mesh's primary now (the same flag its view holds): the
    /// primary authors its members' statuses; every other member takes them from its projection.
    primary: Arc<std::sync::atomic::AtomicBool>,
    /// The holder's own mesh: an own-mesh birth's status comes from that mesh's primary (gossip.md
    /// §3.1 `Members`). Empty for a book no membership holds: no birth is own-mesh to it.
    own_mesh: Arc<Mutex<String>>,
    /// The holder's own NodeId, once it published: its own birth's word about itself is its own.
    own: Arc<Mutex<Option<String>>>,
    /// The status this book holds for each own-mesh birth, by NodeId: what the mesh-primary's
    /// projection said, or, while this book's holder is the primary, what it authored.
    member_statuses: Arc<Mutex<HashMap<String, (IncarnationId, rafka_mesh_entity::MemberStatus)>>>,
    /// The stop and restart overlays that ended here (their birth rejoined), by operation key: a
    /// later copy of the same overlay (a snapshot built before the rejoin, the primary's round
    /// re-applying an operation whose `Complete` is not receipted yet) opens nothing.
    ended_overlays: Arc<Mutex<BTreeSet<(String, u32, String)>>>,
    /// The stops and deletes this node executed (R-NS1): the exact birth of each overlay it published
    /// as `NodeStopped` from the matching stopped reply, or as `NodeDeleted` from the provider's exit
    /// proof, by operation key. Only these ride an aggregate it publishes for a mesh with no primary.
    executed: Arc<Mutex<Executed>>,
    /// The path name of the node holding this book, named by the refusals it writes.
    holder: Arc<Mutex<String>>,
    /// The mesh names the accepted Build holds, once this node holds the Build: the authority on
    /// which meshes the fabric has.
    accepted_meshes: Arc<Mutex<Option<BTreeSet<String>>>>,
    retention: Duration,
    /// The staleness floor.
    staleness_floor: Duration,
    /// The seat holders this node holds (`Frame::Seated`, an entry read): an input to every election.
    pub seats: SeatBook,
    /// The mesh and fabric statuses this node holds (`Frame::MeshStatus`, `Frame::FabricStatus`, a
    /// replay or an entry answer): application routing reads a birth's mesh status here.
    pub statuses: StatusBook,
}

impl Default for DigestBook {
    fn default() -> Self {
        Self::with_retention(DEPARTED_RETENTION)
    }
}

/// The routing facts a book holds, watched: a held member first held, its birth or status changed,
/// heard again after silence; a lifecycle overlay opened or closed; a departure; a mesh status
/// taken. What application
/// routing's view rebuild wakes on, so a fact is applied on receipt (states.md "Routing"). A local
/// observation of the book; nothing is sent.
pub struct RoutingWatch {
    heard: tokio::sync::watch::Receiver<u64>,
    births: tokio::sync::watch::Receiver<u64>,
    overlays: tokio::sync::watch::Receiver<u64>,
    meshes: tokio::sync::watch::Receiver<u64>,
}

impl RoutingWatch {
    /// Every fact moved so far is seen: called right before a rebuild reads the book, so a fact taken
    /// while it reads wakes the next one.
    pub fn seen(&mut self) {
        self.heard.borrow_and_update();
        self.births.borrow_and_update();
        self.overlays.borrow_and_update();
        self.meshes.borrow_and_update();
    }

    /// Returns when a fact moved since [`RoutingWatch::seen`]. A book that is gone moves nothing:
    /// this never returns then.
    pub async fn moved(&mut self) {
        let ok = tokio::select! {
            r = self.heard.changed() => r.is_ok(),
            r = self.births.changed() => r.is_ok(),
            r = self.overlays.changed() => r.is_ok(),
            r = self.meshes.changed() => r.is_ok(),
        };
        if !ok {
            std::future::pending::<()>().await;
        }
    }
}

/// What applying `mesh-left` did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MeshLeftApplied {
    /// The mesh's name, when this book knew it.
    pub name: Option<String>,
    /// How many births departed now.
    pub departed: usize,
}

/// The stops and deletes a node executed, each by (operation key, NodeId, incarnation).
#[derive(Debug, Default)]
struct Executed {
    stops: BTreeSet<((String, u32, String), String, String)>,
    deletes: BTreeSet<((String, u32, String), String, String)>,
}

/// A lifecycle overlay (`NodeStopped`, or a `NodeDeleted` other than the departure held) of a birth
/// already held departed: it arrived after the birth left (its mesh's `MeshLeft`, or its own
/// `NodeDeleted`) and resurrects nothing.
fn reject_departed_overlay(holder: &str, op: &LifecycleOp, frame: &'static str) {
    tracing::info_span!(
        "rdm.mesh.membership.reject.via-departed-birth",
        holder,
        node = %op.name,
        node_id = %op.node_id,
        incarnation_id = %op.incarnation.0,
        frame,
        build_id = %op.build_id,
        attempt = op.attempt,
        operation = %op.operation,
    )
    .in_scope(|| tracing::info!("a lifecycle overlay of a departed birth: refused, the departure stands"));
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

/// How long a member has gone unheard at `now`: the time since its receipt, a member heard directly
/// or on the backbone alike. A member known only from forwarded topology never ages (R-G2:
/// forwarded topology carries topology, never liveness); a terminal `Leaving` is a proven
/// departure, not silence, so it ages from its receipt however it was heard.
fn unheard(d: &MeshDigest, at: &Instant, heard: Heard, now: Instant) -> Duration {
    match heard {
        Heard::Topology if d.status != rafka_mesh_entity::MemberStatus::Leaving => Duration::ZERO,
        _ => now.saturating_duration_since(*at),
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
        Self::with_floor(retention, staleness_floor())
    }

    /// A book with this staleness floor.
    pub(crate) fn with_floor(retention: Duration, staleness_floor: Duration) -> Self {
        Self {
            inner: Arc::default(),
            births: Arc::new(tokio::sync::watch::Sender::new(0)),
            heard: Arc::new(tokio::sync::watch::Sender::new(0)),
            overlays: Arc::new(tokio::sync::watch::Sender::new(0)),
            digests: Arc::new(tokio::sync::watch::Sender::new(0)),
            departed: Arc::default(),
            refused_departures: Arc::default(),
            in_flight: Arc::default(),
            stopped_marks: Arc::default(),
            restarts_carried: Arc::default(),
            primary: Arc::default(),
            own_mesh: Arc::default(),
            own: Arc::default(),
            member_statuses: Arc::default(),
            ended_overlays: Arc::default(),
            executed: Arc::default(),
            holder: Arc::default(),
            accepted_meshes: Arc::default(),
            retention,
            staleness_floor,
            seats: SeatBook::default(),
            statuses: StatusBook::default(),
        }
    }

    /// The staleness floor: a member unheard for longer is `PendingReconnect`.
    pub fn staleness_floor(&self) -> Duration {
        self.staleness_floor
    }

    fn expire_departed(&self) {
        self.expire_departed_at(Instant::now())
    }

    fn expire_departed_at(&self, now: Instant) {
        let mut departed = self.departed.lock().unwrap();
        departed.retain(|_, (_, at)| now.duration_since(*at) < self.retention);
        self.refused_departures.lock().unwrap().retain(|id, _| departed.contains_key(id));
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
            if let Some((held, _)) = departed.get(&id) {
                // The same departure again is a copy (every aggregate carries it for the retention); a
                // different one names a birth that already left: refused by the departed fence, by
                // name once. Its publisher carries it in every aggregate for the retention too, so a
                // later copy of a departure already refused is refused with nothing more said.
                let first_refusal = held.key() != op.key() && self.refused_departures.lock().unwrap().entry(id.clone()).or_default().insert(op.key());
                drop(departed);
                if first_refusal {
                    reject_departed_overlay(&self.holder.lock().unwrap(), &op, "node-deleted");
                }
                return false;
            }
            departed.insert(id.clone(), (op.clone(), now));
        }
        fence_incarnation(&op.node_id, &op.incarnation);
        self.end_stop(op.node_id.as_str());
        self.in_flight.lock().unwrap().remove(&op.key());
        self.overlays_moved();
        self.member_statuses.lock().unwrap().remove(&id);
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
        if self.ended_overlays.lock().unwrap().contains(&op.key()) {
            return false;
        }
        let new = self.in_flight.lock().unwrap().insert(op.key(), op.clone()).is_none();
        if new {
            self.overlays_moved();
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

    /// The restart overlays the Build facts name open, applied as the facts' truth: each is opened
    /// here unless it already ended here, by node.started of its birth at the mesh-primary
    /// ([`Self::started`]) or by the mesh-primary's projection moving past it at any other node
    /// (gossip.md: "NodeRestarting stays until node.started of the same `(node.id, incarnation)`
    /// arrives"); a heartbeat ends none. A restart overlay this book holds also closes on the facts'
    /// word: its operation's `Complete` is receipted in its Build (`completed`, by Build and
    /// operation), its attempt's verdict is `Converged` (`converged`, by Build and attempt: the
    /// fabric-primary records it from the executor's stream terminal, which follows every step's
    /// `Complete`, so progress waits on no gossiped receipt), or its Build is forgotten (`forgotten`).
    /// A hold the facts do not name yet (a `NodeRestarting` frame or a peer's summary arrived before
    /// its receipts) stays open until one of those ends it.
    pub fn restarts_from_facts(&self, open: &[LifecycleOp], completed: &BTreeSet<(String, String)>, converged: &BTreeSet<(String, u32)>, forgotten: &BTreeSet<String>) {
        for op in open.iter().filter(|o| o.is_restart()) {
            self.restarting(op.clone());
        }
        let mut cleared = Vec::new();
        {
            let mut in_flight = self.in_flight.lock().unwrap();
            let mut ended = self.ended_overlays.lock().unwrap();
            in_flight.retain(|k, o| {
                if !o.is_restart() {
                    return true;
                }
                let by = if completed.contains(&(o.build_id.clone(), o.operation.clone())) {
                    "complete-receipted"
                } else if converged.contains(&(o.build_id.clone(), o.attempt)) {
                    "attempt-converged"
                } else if forgotten.contains(&o.build_id) {
                    "build-forgotten"
                } else {
                    return true;
                };
                ended.insert(k.clone());
                cleared.push((o.clone(), by));
                false
            });
        }
        if !cleared.is_empty() {
            self.overlays_moved();
        }
        for (op, by) in cleared {
            tracing::info_span!(
                "rdm.mesh.membership.update.via-restart-closed-by-facts",
                node = %op.name,
                node_id = %op.node_id,
                incarnation_id = %op.incarnation.0,
                build_id = %op.build_id,
                attempt = op.attempt,
                operation = %op.operation,
                by,
            )
            .in_scope(|| tracing::info!("the Build facts end the restart: its Complete is receipted, its attempt Converged, or its Build was forgotten"));
        }
    }

    /// [`Self::restarting`], from a `NodeRestarting` whose seated publisher changed the subject at
    /// `carried`'s version (`(publisher, topology_version)`): the restart is held as carried there.
    fn restarting_carried(&self, op: LifecycleOp, carried: Option<(String, u64)>) -> bool {
        let key = op.key();
        let new = self.restarting(op);
        if let Some((publisher, version)) = carried {
            self.note_restart_carried(&key, &publisher, version);
        }
        new
    }

    /// Hold the open restart `key` as carried at `version` of `publisher`, unless it is held at a
    /// higher version of the same publisher.
    fn note_restart_carried(&self, key: &(String, u32, String), publisher: &str, version: u64) {
        if !self.in_flight.lock().unwrap().contains_key(key) {
            return;
        }
        let mut carried = self.restarts_carried.lock().unwrap();
        if carried.get(key).is_none_or(|(p, v)| p != publisher || version > *v) {
            carried.insert(key.clone(), (publisher.to_string(), version));
        }
    }

    /// A projection frame applied: every open restart the source carried, before the frame at the
    /// version held or after it at the frame's version, is held as carried at the highest such
    /// version of that publisher.
    fn note_restarts_carried(&self, fence: &Fence) {
        if let Some((publisher, version)) = &fence.held {
            for key in &fence.prior_restarts {
                self.note_restart_carried(key, publisher, *version);
            }
        }
        for key in &fence.post_restarts {
            self.note_restart_carried(key, &fence.publisher, fence.version);
        }
    }

    /// The restarts of `mesh` the projection frame `fence` describes moved past: this book holds the
    /// restart carried at some version of the source, this frame is newer than that version (or of the
    /// source's next epoch), and the projection after it no longer carries the restart. Its
    /// mesh-primary took node.started of the birth, and the restart ends here (gossip.md:
    /// "NodeRestarting stays until node.started of the same `(node.id, incarnation)` arrives").
    /// Decided and taken under one hold, so one end is spanned whichever receive path applies it.
    fn restarts_moved_past(&self, mesh: &str, fence: &Fence) {
        let closed: Vec<(LifecycleOp, u64)> = {
            let mut carried = self.restarts_carried.lock().unwrap();
            let mut in_flight = self.in_flight.lock().unwrap();
            let mut ended = self.ended_overlays.lock().unwrap();
            let moved: Vec<((String, u32, String), LifecycleOp, u64)> = in_flight
                .iter()
                .filter(|(k, o)| o.is_restart() && o.name.mesh == mesh && !fence.post_restarts.contains(*k))
                .filter_map(|(k, o)| carried.get(k).filter(|(p, v)| *p != fence.publisher || fence.version > *v).map(|(_, v)| (k.clone(), o.clone(), *v)))
                .collect();
            for (k, _, _) in &moved {
                in_flight.remove(k);
                ended.insert(k.clone());
                carried.remove(k);
            }
            moved.into_iter().map(|(_, o, v)| (o, v)).collect()
        };
        if closed.is_empty() {
            return;
        }
        self.overlays_moved();
        for (op, at) in closed {
            tracing::info_span!(
                "rdm.mesh.membership.update.via-restart-ended-in-projection",
                node = %self.holder.lock().unwrap(),
                member = %op.name,
                node_id = %op.node_id,
                incarnation_id = %op.incarnation.0,
                build_id = %op.build_id,
                attempt = op.attempt,
                operation = %op.operation,
                publisher = %fence.publisher,
                channel = fence.channel,
                held_version = fence.held_version(),
                restart_carried_at = at,
                topology_version = fence.version,
            )
            .in_scope(|| tracing::info!("the mesh-primary's projection moved from a version carrying the restart to one that no longer carries it: node.started of the birth ended it"));
        }
    }

    /// The removal overlays (`NodeDeleting`) the Build facts ended, closed: an overlay is keyed by its
    /// attempt (gossip.md: `(kind, build_id, attempt, operation)`, riding `Members.in_flight` while
    /// the operation is claimed and has no completed receipt), so it closes when its attempt's verdict
    /// is terminal (`ended`, by Build and attempt, each named `attempt-converged` or `attempt-failed`
    /// by `failed`) or its Build is forgotten (`forgotten`). A copy of a closed overlay opens nothing
    /// again. Restarts and stops are not removals: their own facts end them.
    pub fn deletions_from_facts(&self, converged: &BTreeSet<(String, u32)>, failed: &BTreeSet<(String, u32)>, forgotten: &BTreeSet<String>) {
        let mut cleared = Vec::new();
        {
            let mut in_flight = self.in_flight.lock().unwrap();
            let mut ended = self.ended_overlays.lock().unwrap();
            in_flight.retain(|k, o| {
                if o.is_restart() || o.is_stop() {
                    return true;
                }
                let attempt = (o.build_id.clone(), o.attempt);
                let by = if failed.contains(&attempt) {
                    "attempt-failed"
                } else if converged.contains(&attempt) {
                    "attempt-converged"
                } else if forgotten.contains(&o.build_id) {
                    "build-forgotten"
                } else {
                    return true;
                };
                ended.insert(k.clone());
                cleared.push((o.clone(), by));
                false
            });
        }
        if !cleared.is_empty() {
            self.overlays_moved();
            self.births.send_modify(|v| *v += 1);
        }
        for (op, by) in cleared {
            tracing::info_span!(
                "rdm.mesh.membership.update.via-deleting-closed-by-facts",
                node = %op.name,
                node_id = %op.node_id,
                incarnation_id = %op.incarnation.0,
                build_id = %op.build_id,
                attempt = op.attempt,
                operation = %op.operation,
                by,
            )
            .in_scope(|| tracing::info!("the Build facts end the removal's attempt: its overlay closes and the node is held as its digests say"));
        }
    }

    /// The open restart of `node_id`'s birth `incarnation`, if any.
    pub fn restart_of(&self, node_id: &str, incarnation: &rafka_mesh_entity::IncarnationId) -> Option<LifecycleOp> {
        self.in_flight.lock().unwrap().values().find(|op| op.is_restart() && op.node_id.as_str() == node_id && &op.incarnation == incarnation).cloned()
    }

    /// The mesh-primary took node.started of the birth `incarnation` of `node_id`: its
    /// DeclareNodeState(ReadyForTraffic), applied by its StatusAuthority (states.md "Who sets each
    /// state": node ReadyForTraffic is set by "the node's mesh-primary" from "the node's node.started
    /// report, after every start gate"; gossip.md: "NodeRestarting stays until node.started of the same
    /// `(node.id, incarnation)` arrives"). The birth's status is ReadyForTraffic, and an open restart of
    /// that exact birth is over; its projection says both on its next round. Only for a birth of this
    /// book's own mesh: a mesh-primary sets its own mesh's statuses. `false` when nothing moved.
    pub fn started(&self, node_id: &NodeId, incarnation: &IncarnationId) -> bool {
        let held = self.inner.lock().unwrap().get(node_id.as_str()).filter(|(d, _, _)| d.node.incarnation == *incarnation).map(|(d, _, _)| d.node.name.clone());
        let restart = self.restart_of(node_id.as_str(), incarnation);
        let name = held.clone().or(restart.as_ref().map(|o| o.name.clone()));
        let own_mesh = self.own_mesh.lock().unwrap().clone();
        let Some(name) = name.filter(|n| !own_mesh.is_empty() && n.mesh == own_mesh) else { return false };
        let prior = self.author_status(node_id.as_str(), incarnation, rafka_mesh_entity::MemberStatus::ReadyForTraffic);
        let closed: Vec<LifecycleOp> = {
            let mut in_flight = self.in_flight.lock().unwrap();
            let mut ended = self.ended_overlays.lock().unwrap();
            let closed: Vec<LifecycleOp> = in_flight.values().filter(|o| o.is_restart() && o.node_id == *node_id && o.incarnation == *incarnation).cloned().collect();
            for o in &closed {
                in_flight.remove(&o.key());
                ended.insert(o.key());
            }
            closed
        };
        if !closed.is_empty() {
            self.overlays_moved();
            self.heard.send_modify(|v| *v += 1);
        }
        tracing::info_span!(
            "rdm.mesh.membership.update.from-node-started",
            node = %self.holder.lock().unwrap(),
            member = %name,
            node_id = %node_id,
            incarnation_id = %incarnation.0,
            prior = prior.map(|p| format!("{p:?}")).unwrap_or_default(),
            status = "ReadyForTraffic",
            restarts_closed = %closed.iter().map(|o| o.operation.as_str()).collect::<Vec<_>>().join(","),
        )
        .in_scope(|| tracing::info!("the mesh-primary took node.started: the birth is ready-for-traffic and its restart, if one was open, is over; its projection says so"));
        true
    }

    /// Accept a stopped birth (`op` is `stop-node:<path>`, naming the birth that stopped): the node
    /// leaves the book and is not routable, and no heartbeat of that birth is taken again
    /// (`stopped_refuses`). Only the mesh-primary's `node.joined` ([`Self::joined`]), its projection
    /// moving past the stop, a later incarnation of the NodeId or a departure ends it. `false` when
    /// already held, when the node departed, when a later birth of the node is held (the stop is of
    /// an older birth), or when the birth is this book's own holder: a node's own book holds itself,
    /// and its own stop is its own state (a start's topology read carries it before `node.joined`).
    pub fn stopped(&self, op: LifecycleOp) -> bool {
        self.stopped_carried(op, None)
    }

    /// [`Self::stopped`], from a `NodeStopped` whose seated publisher changed the subject at
    /// `carried`'s version (`(publisher, topology_version)`): the stop is held as carried there, so a
    /// later projection of that publisher listing the birth moves past it even when no projection
    /// carrying the stop reached this book.
    fn stopped_carried(&self, op: LifecycleOp, carried: Option<(String, u64)>) -> bool {
        if self.is_departed(op.node_id.as_str()) {
            reject_departed_overlay(&self.holder.lock().unwrap(), &op, "node-stopped");
            return false;
        }
        if self.own.lock().unwrap().as_deref() == Some(op.node_id.as_str()) {
            return false;
        }
        let held_digest = self.inner.lock().unwrap().get(op.node_id.as_str()).map(|(d, _, _)| d.clone());
        let held = held_digest.as_ref().map(|d| (d.node.incarnation.clone(), d.digest_seq, d.status));
        if held.as_ref().is_some_and(|(inc, _, _)| *inc != op.incarnation) {
            return false;
        }
        if self.ended_overlays.lock().unwrap().contains(&op.key()) {
            return false;
        }
        let new = self.in_flight.lock().unwrap().insert(op.key(), op.clone()).is_none();
        if !new {
            return false;
        }
        self.overlays_moved();
        let seq = held.as_ref().map(|(_, seq, _)| *seq).unwrap_or(0);
        self.stopped_marks.lock().unwrap().insert(op.node_id.to_string(), StopMark { incarnation: op.incarnation.clone(), held_seq: seq, held: held_digest, latest: None, carried: carried.clone() });
        // A stopped birth's status is the overlay: the status held for it ends with it.
        self.member_statuses.lock().unwrap().remove(op.node_id.as_str());
        let removed = self.inner.lock().unwrap().remove(op.node_id.as_str()).is_some();
        tracing::info_span!(
            "rdm.mesh.membership.remove.via-node-stopped",
            node = %op.name,
            node_id = %op.node_id,
            incarnation_id = %op.incarnation.0,
            build_id = %op.build_id,
            attempt = op.attempt,
            operation = %op.operation,
            was_held = removed,
            held_status = held.as_ref().map(|(_, _, st)| format!("{st:?}")).unwrap_or_default(),
            held_digest_seq = seq,
            holder = %self.holder.lock().unwrap(),
            publisher = %carried.as_ref().map(|(p, _)| p.as_str()).unwrap_or(""),
            topology_version = carried.as_ref().map(|(_, v)| *v as i64).unwrap_or(-1),
        )
        .in_scope(|| tracing::info!("the exact birth is stopped: dropped from the view; no heartbeat of it is taken until its mesh-primary's node.joined ends the stop"));
        self.births.send_modify(|v| *v += 1);
        true
    }

    /// Whether `d`, arriving through `door`, is refused because its birth is stopped. Every digest of
    /// the stopped incarnation is refused by name, whatever its status or sequence (gossip.md,
    /// "Node-stopped lifecycle overlay": the stop stays until the next lifecycle op on that exact
    /// birth). A digest of another birth of the node ends the stop and is taken by the rules of any
    /// birth.
    fn stopped_refuses(&self, d: &MeshDigest, door: &'static str, now: Instant) -> bool {
        let held_seq = {
            let mut marks = self.stopped_marks.lock().unwrap();
            let Some(mark) = marks.get_mut(d.node.node_id.as_str()) else { return false };
            if mark.incarnation != d.node.incarnation {
                None
            } else {
                if mark.latest.as_ref().is_none_or(|(l, _, _)| d.digest_seq > l.digest_seq) && d.digest_seq > mark.held_seq {
                    let heard = match door {
                        "heartbeat" => Heard::Direct,
                        "forwarded" | "join" => Heard::Forwarded,
                        _ => Heard::Topology,
                    };
                    mark.latest = Some((d.clone(), now, heard));
                }
                Some(mark.held_seq)
            }
        };
        // The status the digest carried: a projection entry's (`topology`, `forwarded`); a heartbeat
        // and a join digest carry none.
        let carried_status = if matches!(door, "topology" | "forwarded") { format!("{:?}", d.status) } else { String::new() };
        let Some(held_seq) = held_seq else {
            self.end_stop(d.node.node_id.as_str());
            tracing::info_span!("rdm.mesh.membership.update.via-node-started", node = %d.node.name, node_id = %d.node.node_id, incarnation_id = %d.node.incarnation.0, node_state = ?d.node_state, digest_seq = d.digest_seq, door)
                .in_scope(|| tracing::info!("a later birth of a stopped node is heard: the stop of the older birth is over"));
            return false;
        };
        tracing::info_span!(
            "rdm.mesh.membership.reject.via-stopped-birth",
            node = %self.holder.lock().unwrap(),
            member = %d.node.name,
            node_id = %d.node.node_id,
            incarnation_id = %d.node.incarnation.0,
            node_state = ?d.node_state,
            entry_status = %carried_status,
            digest_seq = d.digest_seq,
            held_digest_seq = held_seq,
            door,
        )
        .in_scope(|| tracing::info!("a digest of a stopped birth: refused, the stop stands until its mesh-primary's node.joined"));
        true
    }

    /// The mesh-primary took `node.joined` of the birth `op` names on its start (node-start.md step 2:
    /// "node.start clears NodeStopped at node.joined"; states.md: node pending is set by the node's
    /// mesh-primary from the node.joined report): the birth is held `Pending`, and its projection says
    /// so on its next round. A stop of that exact birth ends first, and the birth is held again from
    /// the latest digest heard of it, under `op`'s name (the row's, which the join canonicalized). With
    /// no stop held (a restart's start: its stop publishes no NodeStopped) the own-mesh birth this book
    /// holds of exactly that incarnation is set `Pending` as it stands; its restart stays open until
    /// node.started. `false` when this book holds no stop of that birth and no digest of it of its own
    /// mesh.
    pub fn joined(&self, op: &LifecycleOp) -> bool {
        let mark = self.stopped_marks.lock().unwrap().get(op.node_id.as_str()).cloned().filter(|m| m.incarnation == op.incarnation);
        let cleared: Vec<String> = self.in_flight().into_iter().filter(|o| mark.is_some() && o.is_stop() && o.node_id == op.node_id).map(|o| o.operation).collect();
        let (held_again, held_digest_seq) = match &mark {
            Some(mark) => {
                // The overlay clears before the birth is held again: a projection built in between
                // lists neither, never both.
                self.end_stop(op.node_id.as_str());
                let again = mark.latest.clone().map(|(d, _, _)| d).or(mark.held.clone()).map(|mut d| {
                    d.status = rafka_mesh_entity::MemberStatus::Pending;
                    d.node.name = op.name.clone();
                    d
                });
                if let Some(d) = &again {
                    self.inner.lock().unwrap().insert(op.node_id.to_string(), (d.clone(), Instant::now(), Heard::Direct));
                    self.author_status(op.node_id.as_str(), &op.incarnation, rafka_mesh_entity::MemberStatus::Pending);
                    self.heard.send_modify(|v| *v += 1);
                    self.births.send_modify(|v| *v += 1);
                    self.digests.send_modify(|v| *v += 1);
                }
                (again.is_some(), again.as_ref().map(|d| d.digest_seq).unwrap_or(0))
            }
            None => {
                let own_mesh = self.own_mesh.lock().unwrap().clone();
                let held = self.inner.lock().unwrap().get(op.node_id.as_str()).filter(|(d, _, _)| d.node.incarnation == op.incarnation && !own_mesh.is_empty() && d.node.name.mesh == own_mesh).map(|(d, _, _)| d.digest_seq);
                let Some(seq) = held else { return false };
                self.author_status(op.node_id.as_str(), &op.incarnation, rafka_mesh_entity::MemberStatus::Pending);
                (true, seq)
            }
        };
        tracing::info_span!(
            "rdm.mesh.membership.update.via-node-joined",
            node = %self.holder.lock().unwrap(),
            member = %op.name,
            node_id = %op.node_id,
            incarnation_id = %op.incarnation.0,
            build_id = %op.build_id,
            attempt = op.attempt,
            operation = %op.operation,
            stops_cleared = %cleared.join(","),
            status = "Pending",
            held_again,
            held_digest_seq,
        )
        .in_scope(|| tracing::info!("the mesh-primary took node.joined: the birth is held Pending, its stop, if one was held, is over; its projection says so"));
        true
    }

    /// The mesh-primary applied its own mesh's member's report of `status` for its birth `incarnation`
    /// of `node_id` (node-status.md "Status report": "A node reports to its mesh's primary, which sets
    /// its status"): held for the birth, so its projection carries it from its next round. Its
    /// node.started is [`Self::started`]. `false` when the birth is not of this book's own mesh or this
    /// book holds no digest of exactly that birth.
    pub fn reported(&self, node_id: &NodeId, incarnation: &IncarnationId, status: rafka_mesh_entity::MemberStatus) -> bool {
        let own_mesh = self.own_mesh.lock().unwrap().clone();
        let Some(name) = self.inner.lock().unwrap().get(node_id.as_str()).filter(|(d, _, _)| d.node.incarnation == *incarnation && !own_mesh.is_empty() && d.node.name.mesh == own_mesh).map(|(d, _, _)| d.node.name.clone()) else {
            return false;
        };
        let prior = self.author_status(node_id.as_str(), incarnation, status);
        tracing::info_span!(
            "rdm.mesh.membership.update.from-status-report",
            node = %self.holder.lock().unwrap(),
            member = %name,
            node_id = %node_id,
            incarnation_id = %incarnation.0,
            status = ?status,
            prior = prior.map(|p| format!("{p:?}")).unwrap_or_default(),
        )
        .in_scope(|| tracing::info!("the mesh-primary applied its member's status report: the birth's status is set from it; its projection says so"));
        true
    }

    /// The mesh-primary admitted the command `op` (`drain-node`, `stop-node`) on the birth of its own
    /// mesh `op` names, and sets that birth `status` (states.md "Who sets each state": node
    /// connections-deleting at "the admission of `node.connections.delete { scope: All }`; inside
    /// node.stop the visible status is stopping", node stopping at "the stop call's admission"): held
    /// for the birth, so its projection carries it from its next round. No heartbeat says it (gossip.md
    /// §3.1: a Digest "carries no status"). `false` when the birth is not of this book's own mesh or
    /// this book holds no digest of exactly that birth.
    pub fn admitted(&self, op: &LifecycleOp, status: rafka_mesh_entity::MemberStatus) -> bool {
        let own_mesh = self.own_mesh.lock().unwrap().clone();
        let held = !own_mesh.is_empty() && op.name.mesh == own_mesh && self.inner.lock().unwrap().get(op.node_id.as_str()).is_some_and(|(d, _, _)| d.node.incarnation == op.incarnation);
        if !held {
            return false;
        }
        let prior = self.author_status(op.node_id.as_str(), &op.incarnation, status);
        tracing::info_span!(
            "rdm.mesh.membership.update.from-command-admitted",
            node = %self.holder.lock().unwrap(),
            member = %op.name,
            node_id = %op.node_id,
            incarnation_id = %op.incarnation.0,
            build_id = %op.build_id,
            attempt = op.attempt,
            operation = %op.operation,
            status = ?status,
            prior = prior.map(|p| format!("{p:?}")).unwrap_or_default(),
        )
        .in_scope(|| tracing::info!("the mesh-primary admitted the command: the birth's status is set from it; its projection says so"));
        true
    }

    /// Set `status` for the birth `incarnation` of `node_id`, as its mesh-primary sets it (states.md
    /// "Who sets each state"): held for the birth, so its projection carries it, and given to the
    /// digest held of that exact birth. A move ticks `heard`. Returns the status held for that birth
    /// before (`None` when none was).
    fn author_status(&self, node_id: &str, incarnation: &IncarnationId, status: rafka_mesh_entity::MemberStatus) -> Option<rafka_mesh_entity::MemberStatus> {
        let prior = self.member_statuses.lock().unwrap().insert(node_id.to_string(), (incarnation.clone(), status)).filter(|(inc, _)| inc == incarnation).map(|(_, s)| s);
        if let Some((d, _, _)) = self.inner.lock().unwrap().get_mut(node_id).filter(|(d, _, _)| d.node.incarnation == *incarnation) {
            d.status = status;
        }
        if prior != Some(status) {
            self.heard.send_modify(|v| *v += 1);
        }
        prior
    }

    /// Why the birth `incarnation` publishes nothing any more, as this book holds it: its stop
    /// (`publisher-stopped`; §A.4: "A stopped birth publishes nothing") or its departure
    /// (`publisher-departed`). Any publication of such a birth this book's holder hears was delayed.
    fn publisher_left(&self, incarnation: &IncarnationId) -> Option<&'static str> {
        if self.departed.lock().unwrap().values().any(|(op, _)| &op.incarnation == incarnation) {
            return Some("publisher-departed");
        }
        if self.stopped_marks.lock().unwrap().values().any(|m| &m.incarnation == incarnation) {
            return Some("publisher-stopped");
        }
        None
    }

    /// Whether the birth `d` names is stopped in this book (the exact incarnation).
    fn is_stopped(&self, d: &MeshDigest) -> bool {
        self.stopped_marks.lock().unwrap().get(d.node.node_id.as_str()).is_some_and(|m| m.incarnation == d.node.incarnation)
    }

    /// A projection frame applied: every stopped birth whose stop the source carried, before the
    /// frame at the version held or after it at the frame's version, is marked carried at the highest
    /// such version of that publisher.
    fn note_stops_carried(&self, fence: &Fence) {
        let mut marks = self.stopped_marks.lock().unwrap();
        let mut note = |key: &(String, String), publisher: &str, version: u64| {
            if let Some(m) = marks.get_mut(&key.0).filter(|m| m.incarnation.0 == key.1) {
                if m.carried.as_ref().is_none_or(|(p, v)| p != publisher || version > *v) {
                    m.carried = Some((publisher.to_string(), version));
                }
            }
        };
        if let Some((publisher, version)) = &fence.held {
            for key in &fence.prior_stops {
                note(key, publisher, *version);
            }
        }
        for key in &fence.post_stops {
            note(key, &fence.publisher, fence.version);
        }
    }

    /// The `(publisher, version)` at which a projection this book applied carried the stop of `d`.
    fn stop_carried_at(&self, d: &MeshDigest) -> Option<(String, u64)> {
        self.stopped_marks.lock().unwrap().get(d.node.node_id.as_str()).filter(|m| m.incarnation == d.node.incarnation).and_then(|m| m.carried.clone())
    }

    /// What the projection frame `fence` describes does to the stop of the birth `d` lists: `None`
    /// when this book holds no stop of that exact birth. The projection moved, in order, past the stop
    /// when a projection of the source this book applied carried the stop at some version, this frame
    /// is newer than that version, and the projection after it no longer carries the stop; then the
    /// stop ends here (`Some(true)`, [`Self::stop_ended_in_projection`]). A refresh at the version that
    /// carried it, or a projection that never carried it here, is refused by name (`Some(false)`). A
    /// new publisher's projection is the next epoch of the source (receivers install its full first).
    /// Decided and taken under one hold of the stop marks: a node's backbone and mesh channel each
    /// apply their projections under their own receiver, and the stop ends on one of them, once.
    fn stopped_entry(&self, d: &MeshDigest, fence: &Fence) -> Option<bool> {
        let key = (d.node.node_id.to_string(), d.node.incarnation.0.clone());
        let mark = {
            let mut marks = self.stopped_marks.lock().unwrap();
            let held = marks.get(d.node.node_id.as_str()).filter(|m| m.incarnation == d.node.incarnation)?;
            let moved = held.carried.as_ref().is_some_and(|(publisher, version)| *publisher != fence.publisher || fence.version > *version) && !fence.post_stops.contains(&key);
            if !moved {
                drop(marks);
                self.refuse_in_projection(d, fence);
                return Some(false);
            }
            marks.remove(d.node.node_id.as_str()).expect("the mark is held under this hold")
        };
        self.stop_ended_in_projection(d, fence, mark);
        Some(true)
    }

    /// The mesh-primary's projection of the stopped birth `d`'s mesh moved, in order, from a version
    /// carrying its stop to `version` listing it: its primary took `node.joined`, and the stop ends
    /// here. An own-mesh birth's projection entry gives only its status (its liveness, endpoints and
    /// runtime come from its own Digest), so the birth is held again, as `node.joined` holds it at
    /// the primary, from the latest digest of it this book refused while the stop stood: with the
    /// projection's status, and the instant and channel that digest was heard on (the projection
    /// refreshes no liveness). With none refused, its next heartbeat holds it.
    fn stop_ended_in_projection(&self, d: &MeshDigest, fence: &Fence, mark: StopMark) {
        let cleared: Vec<String> = self.in_flight().into_iter().filter(|o| o.is_stop() && o.node_id == d.node.node_id).map(|o| o.operation).collect();
        let carried = mark.carried.as_ref().map(|(_, v)| *v as i64).unwrap_or(-1);
        let latest = mark.latest;
        self.end_stop(d.node.node_id.as_str());
        let held_again = match latest {
            Some((mut again, at, heard)) if self.own_mesh_member(d) => {
                again.status = d.status;
                let seq = again.digest_seq;
                self.inner.lock().unwrap().insert(d.node.node_id.to_string(), (again, at, heard));
                self.heard.send_modify(|v| *v += 1);
                self.births.send_modify(|v| *v += 1);
                self.digests.send_modify(|v| *v += 1);
                Some(seq)
            }
            _ => None,
        };
        tracing::info_span!(
            "rdm.mesh.membership.update.via-stop-ended-in-projection",
            node = %self.holder.lock().unwrap(),
            member = %d.node.name,
            node_id = %d.node.node_id,
            incarnation_id = %d.node.incarnation.0,
            stops_cleared = %cleared.join(","),
            publisher = %fence.publisher,
            channel = fence.channel,
            held_version = fence.held_version(),
            stop_carried_at = carried,
            topology_version = fence.version,
            status = ?d.status,
            held_again = held_again.is_some(),
            held_digest_seq = held_again.unwrap_or(0),
        )
        .in_scope(|| tracing::info!("the mesh-primary's projection moved from the version carrying the stop to one listing the birth: the stop is over"));
    }

    /// A projection entry of the stopped birth `d` from a version that never carried its stop here:
    /// published before the stop, or a refresh at the version held. Refused by name; the stop stands.
    fn refuse_in_projection(&self, d: &MeshDigest, fence: &Fence) {
        tracing::info_span!(
            "rdm.mesh.membership.reject.via-stopped-birth",
            node = %self.holder.lock().unwrap(),
            member = %d.node.name,
            node_id = %d.node.node_id,
            incarnation_id = %d.node.incarnation.0,
            status = ?d.status,
            digest_seq = d.digest_seq,
            door = "projection",
            publisher = %fence.publisher,
            channel = fence.channel,
            held_version = fence.held_version(),
            topology_version = fence.version,
            stop_carried_at = self.stop_carried_at(d).map(|(_, v)| v as i64).unwrap_or(-1),
        )
        .in_scope(|| tracing::info!("a projection entry of a stopped birth from a version that never carried its stop here: refused, the stop stands"));
    }

    /// Whether `d` is a birth of this book's own mesh other than its holder: its status comes from
    /// that mesh's primary (gossip.md §3.1 `Members`).
    fn own_mesh_member(&self, d: &MeshDigest) -> bool {
        let own_mesh = self.own_mesh.lock().unwrap();
        let itself = self.own.lock().unwrap().as_deref() == Some(d.node.node_id.as_str()) || d.node.name.to_string() == *self.holder.lock().unwrap();
        !own_mesh.is_empty() && d.node.name.mesh == *own_mesh && !itself
    }

    /// The status this book holds for the own-mesh birth `d`, as a non-primary takes it: its
    /// primary's projection's, else `Pending` (a birth whose status this book was not told is not
    /// routable, states.md "Routing").
    fn projected_status(&self, d: &MeshDigest) -> rafka_mesh_entity::MemberStatus {
        self.member_statuses.lock().unwrap().get(d.node.node_id.as_str()).filter(|(inc, _)| *inc == d.node.incarnation).map(|(_, s)| *s).unwrap_or(rafka_mesh_entity::MemberStatus::Pending)
    }

    /// The mesh-primary's projection lists the own-mesh birth `d` with its status (gossip.md §3.1:
    /// own-mesh members receive authoritative statuses through the primary's versioned member
    /// projection): the status is held for the birth, and the held digest, liveness, endpoints and
    /// runtime as its own heartbeat said, takes that status. Nothing else of `d` is taken. `false`
    /// when nothing moved.
    fn take_projected_status(&self, d: &MeshDigest, fence: &Fence) -> bool {
        // This node's own birth is its own word: no projection moves the status it holds of itself.
        if !self.own_mesh_member(d) || self.is_departed(d.node.node_id.as_str()) || self.is_stopped(d) {
            return false;
        }
        let prior = {
            let mut statuses = self.member_statuses.lock().unwrap();
            let prior = statuses.get(d.node.node_id.as_str()).filter(|(inc, _)| *inc == d.node.incarnation).map(|(_, s)| *s);
            if prior == Some(d.status) {
                return false;
            }
            statuses.insert(d.node.node_id.to_string(), (d.node.incarnation.clone(), d.status));
            prior
        };
        let held = {
            let mut inner = self.inner.lock().unwrap();
            match inner.get_mut(d.node.node_id.as_str()) {
                Some((h, _, _)) if h.node.incarnation == d.node.incarnation => {
                    h.status = d.status;
                    true
                }
                _ => false,
            }
        };
        if held {
            self.heard.send_modify(|v| *v += 1);
        }
        tracing::info_span!(
            "rdm.mesh.membership.update.via-projected-status",
            node = %self.holder.lock().unwrap(),
            member = %d.node.name,
            member_id = %d.node.node_id,
            incarnation_id = %d.node.incarnation.0,
            status = ?d.status,
            prior = prior.map(|p| format!("{p:?}")).unwrap_or_default(),
            heartbeat_held = held,
            publisher = %fence.publisher,
            channel = fence.channel,
            topology_version = fence.version,
        )
        .in_scope(|| tracing::info!("an own-mesh member's status taken from its mesh-primary's projection; its liveness stays its own heartbeat's"));
        true
    }

    /// The last digest this book held of each stopped birth: a node-admin keeps showing the birth,
    /// `Leaving`, in its view though the book dropped it. A birth this book never held a digest of
    /// (a late joiner) is not listed.
    pub fn parked(&self) -> Vec<MeshDigest> {
        self.stopped_marks.lock().unwrap().values().filter_map(|m| m.held.clone()).collect()
    }

    /// The open stop overlays of stopped births this book never held a digest of (an admin that
    /// joined after the stop and read the overlay from the mesh topology): the only facts it has of
    /// them are the overlay's own, the node's path, id and incarnation.
    pub fn parked_unheard(&self) -> Vec<LifecycleOp> {
        let marks = self.stopped_marks.lock().unwrap();
        self.in_flight
            .lock()
            .unwrap()
            .values()
            .filter(|op| op.is_stop() && marks.get(op.node_id.as_str()).is_some_and(|m| m.incarnation == op.incarnation && m.held.is_none()))
            .cloned()
            .collect()
    }

    /// The stop of `node_id` is over (its mesh-primary took node.joined, a later birth was heard, or
    /// it departed): its overlay and mark clear.
    fn end_stop(&self, node_id: &str) {
        self.stopped_marks.lock().unwrap().remove(node_id);
        let cleared = {
            let mut in_flight = self.in_flight.lock().unwrap();
            let mut ended = self.ended_overlays.lock().unwrap();
            let before = in_flight.len();
            in_flight.retain(|k, o| {
                let gone = o.is_stop() && o.node_id.as_str() == node_id;
                if gone {
                    ended.insert(k.clone());
                }
                !gone
            });
            in_flight.len() != before
        };
        if cleared {
            self.overlays_moved();
        }
    }

    /// An overlay was added or cleared, or a departure accepted: the overlay watch ticks.
    fn overlays_moved(&self) {
        self.overlays.send_modify(|v| *v += 1);
    }

    /// Ticks each time an open lifecycle overlay is added or cleared, or a departure is accepted:
    /// what a mesh primary's publication round wakes on when its aggregate's overlays moved.
    pub fn overlay_changes(&self) -> tokio::sync::watch::Receiver<u64> {
        self.overlays.subscribe()
    }

    /// Accept an open lifecycle overlay: the node stays held and stops being
    /// routable. `false` when the overlay was already held or the node has
    /// already departed.
    pub fn deleting(&self, op: LifecycleOp) -> bool {
        if self.is_departed(op.node_id.as_str()) {
            return false;
        }
        // Applied once however many copies arrive (gossip.md `NodeDeleting`): a copy of an overlay
        // the Build facts ended opens nothing again.
        if self.ended_overlays.lock().unwrap().contains(&op.key()) {
            return false;
        }
        let new = self.in_flight.lock().unwrap().insert(op.key(), op.clone()).is_none();
        if new {
            self.overlays_moved();
            // What this receiver holds of the node as the overlay opens: found, in what status, and
            // not routable (the overlay is what makes it so).
            let held_status = self.inner.lock().unwrap().get(op.node_id.as_str()).map(|(d, _, _)| format!("{:?}", d.status));
            tracing::info_span!(
                "rdm.mesh.membership.update.via-node-deleting",
                node = %op.name,
                node_id = %op.node_id,
                build_id = %op.build_id,
                attempt = op.attempt,
                operation = %op.operation,
                held = held_status.is_some(),
                held_status = held_status.as_deref().unwrap_or("not held"),
                routable = self.routable(op.node_id.as_str()),
            )
            .in_scope(|| tracing::info!("the node is being removed: held, not routable"));
            self.births.send_modify(|v| *v += 1);
        }
        new
    }

    /// Hold the mesh names the accepted Build names: the authority on which meshes the fabric has.
    pub fn set_accepted_meshes(&self, meshes: BTreeSet<String>) {
        *self.accepted_meshes.lock().unwrap() = Some(meshes);
    }

    /// Whether the accepted Build names `mesh`; `None` while this node holds no Build.
    pub fn accepted_has_mesh(&self, mesh: &str) -> Option<bool> {
        self.accepted_meshes.lock().unwrap().as_ref().map(|m| m.contains(mesh))
    }

    /// The name of the mesh `mesh_id` names: a node-admin's digest states the mesh id, an ordinary
    /// node's does not; the mesh's births are every held digest of the name the id-carrying digests
    /// bear. A parked birth's last digest counts: a member the leaving mesh stopped is dropped from
    /// the view, and its digest still names its mesh.
    fn mesh_name_of(&self, mesh_id: &MeshId) -> Option<String> {
        self.all()
            .into_iter()
            .chain(self.parked())
            .find(|d| d.mesh_id.as_ref() == Some(mesh_id))
            .map(|d| d.node.name.mesh.clone())
    }

    /// The held digests of the mesh named `name` and carrying no other mesh's id.
    fn births_named(&self, name: &str, mesh_id: &MeshId) -> Vec<MeshDigest> {
        self.all().into_iter().filter(|d| d.node.name.mesh == name && d.mesh_id.as_ref().is_none_or(|m| m == mesh_id)).collect()
    }

    /// `mesh-left` for `mesh_id`, or for the mesh `name` when the caller knows it from the source
    /// projection it holds: every birth of the mesh this book holds, in the view or parked by its
    /// stop, departs through the departure every node's stop takes, and its resolution answers Gone
    /// for the departed retention. Nothing is kept of the mesh. Idempotent.
    pub fn mesh_left(&self, mesh_id: &MeshId, name: Option<&str>, build_id: &str, attempt: u32, at_rafka_ms: u64) -> MeshLeftApplied {
        let name = self.mesh_name_of(mesh_id).or_else(|| name.map(String::from));
        let mut roster: BTreeMap<String, (IncarnationId, rafka_mesh_entity::PathName)> = BTreeMap::new();
        if let Some(n) = &name {
            for d in self.births_named(n, mesh_id) {
                roster.insert(d.node.node_id.to_string(), (d.node.incarnation.clone(), d.node.name.clone()));
            }
            for op in self.in_flight().into_iter().filter(|op| op.is_stop() && op.name.mesh == *n) {
                roster.entry(op.node_id.to_string()).or_insert((op.incarnation.clone(), op.name.clone()));
            }
        }
        let mut departed = 0;
        for (id, (incarnation, path)) in roster {
            let Ok(node_id) = NodeId::parse(&id) else { continue };
            let op = LifecycleOp { build_id: build_id.to_string(), attempt, operation: format!("stop-node:{path}"), node_id, incarnation, name: path, event_at_rafka_ms: at_rafka_ms };
            if self.depart_quiet(op) {
                departed += 1;
            }
        }
        self.births.send_modify(|v| *v += 1);
        MeshLeftApplied { name, departed }
    }

    fn depart_quiet(&self, op: LifecycleOp) -> bool {
        let id = op.node_id.to_string();
        {
            let mut departed = self.departed.lock().unwrap();
            if departed.contains_key(&id) {
                return false;
            }
            departed.insert(id.clone(), (op.clone(), Instant::now()));
        }
        fence_incarnation(&op.node_id, &op.incarnation);
        self.end_stop(op.node_id.as_str());
        self.in_flight.lock().unwrap().retain(|_, o| o.node_id != op.node_id);
        self.overlays_moved();
        self.member_statuses.lock().unwrap().remove(&id);
        self.inner.lock().unwrap().remove(&id);
        true
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

    /// This node published `op` as `NodeStopped` from the matching stopped reply of a stop it
    /// executed: it holds that birth's stop receipt.
    pub fn executed_stop(&self, op: &LifecycleOp) {
        self.executed.lock().unwrap().stops.insert((op.key(), op.node_id.to_string(), op.incarnation.0.clone()));
        // What its publications for a mesh with no primary list moved: a round looks at once.
        self.overlays_moved();
    }

    /// This node published `op` as `NodeDeleted` from the provider's exit proof of a delete it
    /// executed: it holds that birth's exit proof.
    pub fn executed_delete(&self, op: &LifecycleOp) {
        self.executed.lock().unwrap().deletes.insert((op.key(), op.node_id.to_string(), op.incarnation.0.clone()));
        // What its publications for a mesh with no primary list moved: a round looks at once.
        self.overlays_moved();
    }

    /// The meshes other than `own` for which this book holds an open stop overlay or a retained
    /// departure this node executed, each with exactly those overlays and departures: what this node
    /// publishes as that mesh's aggregate while it is the mesh's publisher of record (R-NS1). A stop
    /// that ended (the birth rejoined, or departed) and a departure past retention list nothing; a
    /// birth this node did not stop or delete is never listed.
    pub fn executed_overlays_beyond(&self, own: &str) -> BTreeMap<String, (Vec<LifecycleOp>, Vec<LifecycleOp>)> {
        let (in_flight, departed) = (self.in_flight(), self.departed());
        let mark = |op: &LifecycleOp| (op.key(), op.node_id.to_string(), op.incarnation.0.clone());
        let (open, held): (BTreeSet<_>, BTreeSet<_>) = (in_flight.iter().map(mark).collect(), departed.iter().map(mark).collect());
        let mut executed = self.executed.lock().unwrap();
        // A mark whose overlay ended (a start, a departure, the retention) is never listed again.
        executed.stops.retain(|m| open.contains(m));
        executed.deletes.retain(|m| held.contains(m));
        let stops: Vec<LifecycleOp> = in_flight.into_iter().filter(|op| op.is_stop() && op.name.mesh != own && executed.stops.contains(&mark(op))).collect();
        let deletes: Vec<LifecycleOp> = departed.into_iter().filter(|op| op.name.mesh != own && executed.deletes.contains(&mark(op))).collect();
        let mut out: BTreeMap<String, (Vec<LifecycleOp>, Vec<LifecycleOp>)> = BTreeMap::new();
        for op in stops {
            out.entry(op.name.mesh.clone()).or_default().0.push(op);
        }
        for op in deletes {
            out.entry(op.name.mesh.clone()).or_default().1.push(op);
        }
        out
    }

    /// The open overlays and retained departures of `mesh`'s own nodes: what
    /// that mesh's primary publishes for it.
    pub fn overlays_of(&self, mesh: &str) -> (Vec<LifecycleOp>, Vec<LifecycleOp>) {
        let mine = |ops: Vec<LifecycleOp>| ops.into_iter().filter(|op| op.name.mesh == mesh).collect::<Vec<_>>();
        (mine(self.in_flight()), mine(self.departed()))
    }

    /// May application routing select this node (states.md "Routing"): only while the birth is
    /// ready-for-traffic and its mesh is ready-for-traffic, as this book holds them. Anything else is
    /// a routing stop from the moment this book holds the fact that says so: no held digest, a
    /// departure, an open lifecycle overlay (NodeStopped, NodeRestarting, NodeDeleting), a held
    /// status other than ready-for-traffic, or a mesh status not held ready-for-traffic. A stop is
    /// never a barrier: the same birth's later ready-for-traffic, or its mesh's, restores selection.
    /// The resolver and the Node RPC client never read this; only application routing does.
    pub fn routable(&self, node_id: &str) -> bool {
        if self.is_departed(node_id) || self.in_flight.lock().unwrap().values().any(|op| op.node_id.as_str() == node_id) {
            return false;
        }
        let Some(mesh) = self.inner.lock().unwrap().get(node_id).filter(|(d, _, _)| d.status == rafka_mesh_entity::MemberStatus::ReadyForTraffic).map(|(d, _, _)| d.node.name.mesh.clone()) else {
            return false;
        };
        self.statuses.mesh(&mesh).is_some_and(|m| m.status == MESH_READY_FOR_TRAFFIC)
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

    /// [`Self::record`], heard at `now`. `d` is a heartbeat: the member's liveness, endpoints and
    /// runtime fact. A heartbeat of a stopped birth is refused by name. For a birth of this book's own
    /// mesh, its status is the mesh-primary's (gossip.md §3.1: a Digest "carries no status: a member's
    /// status comes only from its mesh-primary"): the held status is the one this book holds for the
    /// birth ([`Self::projected_status`]), which the primary sets from node.joined, node.started and
    /// the commands it admits, and every other member takes from its primary's projection. No
    /// heartbeat moves it, at the primary or anywhere.
    pub fn record_at(&self, d: MeshDigest, now: Instant) -> bool {
        if self.is_departed_at(d.node.node_id.as_str(), now) {
            reject_departed(&d, "mesh-channel");
            return false;
        }
        if self.stopped_refuses(&d, "heartbeat", now) {
            return false;
        }
        let mut stored = d.clone();
        if self.own_mesh_member(&d) {
            stored.status = self.projected_status(&d);
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
        let birth_changed = inner.get(d.node.node_id.as_str()).is_none_or(|(held, _, _)| held.node != stored.node);
        let heard_changed = inner.get(d.node.node_id.as_str()).is_none_or(|(held, at, _)| held.node != stored.node || held.status != stored.status || now.saturating_duration_since(*at) > self.staleness_floor);
        inner.insert(d.node.node_id.to_string(), (stored, now, Heard::Direct));
        drop(inner);
        if heard_changed {
            self.heard.send_modify(|v| *v += 1);
        }
        if birth_changed {
            self.births.send_modify(|v| *v += 1);
        }
        self.digests.send_modify(|v| *v += 1);
        true
    }

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
        self.record_topology_at(d, Instant::now())
    }

    /// [`Self::record_topology`], received at `now`.
    pub fn record_topology_at(&self, d: MeshDigest, now: Instant) -> bool {
        if self.is_departed_at(d.node.node_id.as_str(), now) {
            reject_departed(&d, "forwarded");
            return false;
        }
        if self.stopped_refuses(&d, "topology", now) {
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
        let heard_changed = inner.get(d.node.node_id.as_str()).is_none_or(|(held, _, _)| held.node != d.node || held.status != d.status);
        let (at, heard) = held_entry.unwrap_or((now, Heard::Topology));
        inner.insert(d.node.node_id.to_string(), (d.clone(), at, heard));
        drop(inner);
        if heard_changed {
            self.heard.send_modify(|v| *v += 1);
        }
        if birth_changed {
            self.births.send_modify(|v| *v += 1);
        }
        self.digests.send_modify(|v| *v += 1);
        true
    }

    /// [`Self::record_forwarded`], heard at `now`.
    pub fn record_forwarded_at(&self, d: MeshDigest, now: Instant) -> bool {
        self.record_forwarded_inner(d, now, Carried::Status)
    }

    /// Hold the digest a birth presented at its join (`JoinNode`, at the admin that deployed it or
    /// commanded its start), as forwarded: its address and liveness, and no status, for a join digest
    /// carries none. An own-mesh birth keeps the status this book holds for it; a first start's, which
    /// it holds none for, is `Pending` (states.md "Who sets each state": node pending, from the node's
    /// node.joined report).
    pub fn record_joined(&self, d: MeshDigest) -> bool {
        self.record_forwarded_inner(d, Instant::now(), Carried::NoStatus)
    }

    fn record_forwarded_inner(&self, mut d: MeshDigest, now: Instant, carried: Carried) -> bool {
        if self.is_departed_at(d.node.node_id.as_str(), now) {
            reject_departed(&d, "forwarded");
            return false;
        }
        if self.stopped_refuses(&d, if carried == Carried::Status { "forwarded" } else { "join" }, now) {
            return false;
        }
        let own_mesh = self.own_mesh_member(&d);
        if carried == Carried::NoStatus && own_mesh {
            d.status = self.projected_status(&d);
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
        let heard_changed = inner.get(d.node.node_id.as_str()).is_none_or(|(held, _, _)| held.node != d.node || held.status != d.status);
        inner.insert(d.node.node_id.to_string(), (d.clone(), now, Heard::Forwarded));
        drop(inner);
        // An own-mesh birth held from what its mesh-primary serves (a topology read, each entry its
        // status): that status is held. A join digest carries none: the held status stands, and a
        // first start's, which this book holds none for, is set Pending by its mesh-primary from that
        // node.joined report.
        if own_mesh && carried == Carried::Status {
            self.member_statuses.lock().unwrap().insert(d.node.node_id.to_string(), (d.node.incarnation.clone(), d.status));
        }
        let first = carried == Carried::NoStatus && own_mesh && self.primary.load(Ordering::Relaxed) && !self.member_statuses.lock().unwrap().get(d.node.node_id.as_str()).is_some_and(|(inc, _)| *inc == d.node.incarnation);
        if first {
            self.author_status(d.node.node_id.as_str(), &d.node.incarnation, rafka_mesh_entity::MemberStatus::Pending);
            tracing::info_span!(
                "rdm.mesh.membership.update.from-join-report",
                node = %self.holder.lock().unwrap(),
                member = %d.node.name,
                node_id = %d.node.node_id,
                incarnation_id = %d.node.incarnation.0,
                node_state = ?d.node_state,
                digest_seq = d.digest_seq,
                status = "Pending",
            )
            .in_scope(|| tracing::info!("the mesh-primary took a first start's join report: the birth is held Pending; its projection says so"));
        }
        if heard_changed {
            self.heard.send_modify(|v| *v += 1);
        }
        if birth_changed {
            self.births.send_modify(|v| *v += 1);
        }
        self.digests.send_modify(|v| *v += 1);
        true
    }

    /// Ticks each time a member's birth is first held or changes (a new
    /// incarnation, a moved address, a runtime published): what a process's
    /// live resolver is fed from. A fresher digest of the same birth does not.
    pub fn birth_changes(&self) -> tokio::sync::watch::Receiver<u64> {
        self.births.subscribe()
    }

    /// The routing facts this book holds, watched ([`RoutingWatch`]).
    pub fn routing_watch(&self) -> RoutingWatch {
        RoutingWatch { heard: self.heard.subscribe(), births: self.births.subscribe(), overlays: self.overlays.subscribe(), meshes: self.statuses.mesh_changes() }
    }

    /// Ticks each time a member is first held, changes its birth or its status, or is heard again
    /// after it had been silent past the staleness floor. The value is a count: a waiter that
    /// recorded it before an event sees the count move after the event, however the two
    /// interleave with its subscribing.
    pub fn heard_changes(&self) -> tokio::sync::watch::Receiver<u64> {
        self.heard.subscribe()
    }

    /// Ticks each time a digest is taken (any of [`Self::record`], a forwarded or a topology copy
    /// that moved the held digest). The value is a count, as [`Self::heard_changes`].
    pub fn digest_changes(&self) -> tokio::sync::watch::Receiver<u64> {
        self.digests.subscribe()
    }

    /// The digests heard within `fresh`.
    pub fn current(&self, fresh: Duration) -> Vec<MeshDigest> {
        self.current_at(fresh, Instant::now())
    }

    /// Digests heard within `fresh` of `now`. A graceful departure (a terminal `Leaving`, then
    /// nothing for one gossip interval) is gone within that tick (fabric-node-lifecycle.md §6): it
    /// is not current, so no view and no aggregate carries it on.
    pub(crate) fn current_at(&self, fresh: Duration, now: Instant) -> Vec<MeshDigest> {
        self.inner
            .lock()
            .unwrap()
            .values()
            .filter(|(d, at, fw)| unheard(d, at, *fw, now) <= fresh)
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
        self.inner.lock().unwrap().get(node_id).map(|(d, at, fw)| (d.clone(), unheard(d, at, *fw, now)))
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

    /// `mesh`'s current primary node-admin: the exact birth the held seat record names, the
    /// canonical LeadershipView's answer as its holder announced it (`Frame::Seated`, an entry
    /// read's seats), with that birth's latest digest. Its status and how long it has gone unheard
    /// do not move the seat (only a new record or a proven loss does), so neither is read here.
    pub fn mesh_primary(&self, mesh: &str) -> Result<MeshDigest, NoMeshPrimary> {
        let holder = self.seats.mesh(mesh).ok_or_else(|| NoMeshPrimary::NoSeatRecord { mesh: mesh.to_string() })?;
        if self.seats.gone().contains(&holder.incarnation) {
            return Err(NoMeshPrimary::HolderGone { holder });
        }
        match self.get(&holder.node_id.to_string()) {
            Some((d, _)) if d.node.incarnation == holder.incarnation => Ok(d),
            Some((d, _)) => Err(NoMeshPrimary::HolderNotHeld { held_incarnation: Some(d.node.incarnation), departed: false, holder }),
            None => Err(NoMeshPrimary::HolderNotHeld { held_incarnation: None, departed: self.is_departed(&holder.node_id.to_string()), holder }),
        }
    }

    /// Every held member with how long it has gone unheard at `now` (a departed birth is not held).
    pub(crate) fn silent_held(&self, now: Instant) -> Vec<(MeshDigest, Duration)> {
        self.inner.lock().unwrap().values().map(|(d, at, fw)| (d.clone(), unheard(d, at, *fw, now))).collect()
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
            node_state: Default::default(),
            admin_api_base: None,
            emitted_at_rafka_ms: at,
            digest_seq: at,
            mesh_id: None,
            in_flight: None,
            config: Default::default(),
            load: None,
            gossip: None,
            data_dir: None,
        }
    }

    /// CONTRACT: the gossip stats a digest carries never reach the topology view the version
    /// orders, so a changed count is no topology change.
    #[test]
    fn topology_view_of_a_digest_with_gossip_stats_equals_the_view_without() {
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        let plain = digest(&id, &inc, None, MemberStatus::ReadyForTraffic, 1);
        let mut with = plain.clone();
        with.gossip = Some(rafka_mesh_entity::GossipStats { heard: 3, neighbours: 2, frames_sent: 10, frames_received: 12 });
        assert_ne!(plain, with);
        assert_eq!(crate::snapshot::topology_view(&with), crate::snapshot::topology_view(&plain));
        assert_eq!(crate::snapshot::without_load(&with).gossip, None);
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
        let (marks, lookup) = (LocationMarks::default(), MemoryLookup::new());
        let addrs = |l: &MemoryLookup| l.get_endpoint_info(key).map(|e| e.ip_addrs().cloned().collect::<Vec<_>>()).unwrap_or_default();
        register_location(&marks, &lookup, &at(41001, &first, None, 40));
        register_location(&marks, &lookup, &at(41002, &second, Some(first.clone()), 1));
        assert_eq!(addrs(&lookup), vec![std::net::SocketAddr::from(([127, 0, 0, 1], 41002))], "the newer birth replaces, never joins, the old socket");
        register_location(&marks, &lookup, &at(41001, &first, None, 41));
        assert_eq!(addrs(&lookup), vec![std::net::SocketAddr::from(([127, 0, 0, 1], 41002))], "a late digest of the old birth never restores its socket");
    }

    /// CONTRACT (ops-naming, `node.deleted`): a new node over the data dir of a deleted one keeps
    /// the endpoint key and sets no `supersedes`, so the location book cannot tell the deleted
    /// instance's late digest from a newer one. The departure of `(node.id, incarnation)` fences
    /// that incarnation in the book: its digest is refused by name and the key keeps the new
    /// instance's socket. Must NOT happen: the key pointed back at the deleted instance's old port.
    #[test]
    fn a_deleted_incarnations_late_digest_never_moves_its_keys_address_back() {
        let key = iroh::SecretKey::generate().public();
        let (old_node, new_node) = (NodeId::mint(), NodeId::mint());
        let (old, new) = (IncarnationId::mint(), IncarnationId::mint());
        let at = |node: &NodeId, port: u16, incarnation: &IncarnationId, seq: u64| {
            let mut d = digest(node, incarnation, None, MemberStatus::ReadyForTraffic, seq);
            d.node.endpoint_id = EndpointId(key.to_string());
            d.node.transport_addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));
            d
        };
        let (marks, lookup) = (LocationMarks::default(), MemoryLookup::new());
        let addrs = |l: &MemoryLookup| l.get_endpoint_info(key).map(|e| e.ip_addrs().cloned().collect::<Vec<_>>()).unwrap_or_default();
        let (port_old, port_new) = (std::net::SocketAddr::from(([127, 0, 0, 1], 41011)), std::net::SocketAddr::from(([127, 0, 0, 1], 41012)));
        register_location(&marks, &lookup, &at(&old_node, 41011, &old, 40));
        register_location(&marks, &lookup, &at(&new_node, 41012, &new, 1));
        assert_eq!(addrs(&lookup), vec![port_new], "the new instance's digest replaces the key's address");
        let book = DigestBook::default();
        assert!(book.depart(op(&old_node, &old, "retire-node:mesh1.rpc.1")));
        assert!(register_location(&marks, &lookup, &at(&old_node, 41011, &old, 41)).is_none());
        assert_eq!(addrs(&lookup), vec![port_new], "a digest of the deleted incarnation is refused: {port_old} is not restored");
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

    /// `mesh`'s status as its primary published it, held by `book` (the frame a peer receives).
    fn hold_mesh_status(book: &DigestBook, mesh: &str, status: &str, changed_at_rafka_ms: u64) {
        let f = Frame::MeshStatus { mesh: mesh.into(), status: status.into(), publisher: format!("{mesh}.admin.1"), forwarded_by: None, changed_at_rafka_ms };
        assert!(book.statuses.take(&f, &FabricId::parse("fab000000001").unwrap()), "{f:?}");
    }

    /// CONTRACT (states.md "Routing"): application routing selects a birth only while it is
    /// ready-for-traffic and its mesh is ready-for-traffic; anything else is a routing stop, and a
    /// later ready-for-traffic for the same exact birth restores selection. A `Pending` birth of a
    /// ready mesh is not selected; its own ready-for-traffic selects it; its `Draining` stops it; the
    /// same birth saying ready-for-traffic again selects it again. Must NOT happen: a `Pending`
    /// birth selected, or a stop that outlives the birth's later ready-for-traffic.
    #[test]
    fn a_birth_is_routable_only_while_it_says_ready_for_traffic_and_a_later_ready_restores_it() {
        let book = DigestBook::default();
        hold_mesh_status(&book, "mesh1", "ready-for-traffic", 10);
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        assert!(book.record(digest(&id, &inc, None, MemberStatus::Pending, 100)));
        assert!(!book.routable(id.as_str()), "a Pending birth is a routing stop");
        assert!(book.record(digest(&id, &inc, None, MemberStatus::ReadyForTraffic, 110)));
        assert!(book.routable(id.as_str()), "the birth and its mesh are ready-for-traffic");
        assert!(book.record(digest(&id, &inc, None, MemberStatus::Draining, 120)));
        assert!(!book.routable(id.as_str()), "a Draining birth is a routing stop");
        assert!(book.record(digest(&id, &inc, None, MemberStatus::ReadyForTraffic, 130)), "the same birth's later ready-for-traffic is taken");
        assert!(book.routable(id.as_str()), "a routing stop is no barrier: the later ready-for-traffic restores selection");
    }

    /// CONTRACT (states.md "Routing"): a ready-for-traffic birth is selected only while its mesh's
    /// status this node holds is ready-for-traffic. No mesh status held, a mesh `pending`, `leaving`
    /// or `dead`, or another mesh's ready-for-traffic: not selected. The mesh's later
    /// ready-for-traffic restores selection from the moment it is held. Must NOT happen: a birth of a
    /// mesh not held ready-for-traffic selected.
    #[test]
    fn a_ready_birth_is_routable_only_while_its_mesh_is_held_ready_for_traffic() {
        let book = DigestBook::default();
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        assert!(book.record(digest(&id, &inc, None, MemberStatus::ReadyForTraffic, 100)));
        assert!(!book.routable(id.as_str()), "no status of mesh1 is held: mesh1 is not held ready-for-traffic");
        hold_mesh_status(&book, "mesh2", "ready-for-traffic", 5);
        assert!(!book.routable(id.as_str()), "another mesh's ready-for-traffic selects nothing in mesh1");
        hold_mesh_status(&book, "mesh1", "pending", 10);
        assert!(!book.routable(id.as_str()), "a pending mesh is a routing stop");
        hold_mesh_status(&book, "mesh1", "ready-for-traffic", 20);
        assert!(book.routable(id.as_str()), "the mesh's ready-for-traffic, received, restores selection");
        hold_mesh_status(&book, "mesh1", "leaving", 30);
        assert!(!book.routable(id.as_str()), "a leaving mesh is a routing stop on receipt, whatever its member says");
        hold_mesh_status(&book, "mesh1", "dead", 40);
        assert!(!book.routable(id.as_str()), "a dead mesh is a routing stop");
    }

    /// CONTRACT (states.md "Routing", gossip.md MeshStatus): the mesh primary that observes its mesh
    /// leaving sends that change at once on the existing MeshStatus send, and a peer that takes the
    /// frame stops selecting the mesh's births on receipt, whatever each member still says. Must NOT
    /// happen: the change waiting for anything, or a peer still selecting a birth of the leaving mesh.
    #[test]
    fn a_mesh_primary_observing_its_mesh_leaving_sends_it_at_once_and_a_peer_stops_routing_on_receipt() {
        let (now, fabric) = (Instant::now(), FabricId::parse("fab000000001").unwrap());
        let mut publisher = StatusPublisher::new("mesh1.admin.1", StatusScope::Mesh("mesh1".into()));
        publisher.set_role(true);
        let peer = DigestBook::default();
        let ready = publisher.observe("ready-for-traffic", None, true, now, 10).expect("the first status is a change");
        assert!(peer.statuses.take(&ready, &fabric));
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        assert!(peer.record(digest(&id, &inc, None, MemberStatus::ReadyForTraffic, 100)));
        assert!(peer.routable(id.as_str()));
        let leaving = publisher.observe("leaving", None, true, now, 20).expect("the mesh's leaving is a change, sent at once");
        assert!(matches!(&leaving, Frame::MeshStatus { status, publisher, .. } if status == "leaving" && publisher == "mesh1.admin.1"), "{leaving:?}");
        assert!(peer.statuses.take(&leaving, &fabric));
        assert!(!peer.routable(id.as_str()), "a peer holding the mesh leaving selects none of its births, though the member still says ready-for-traffic");
    }

    /// Whether `w` says a routing fact moved, without waiting.
    async fn moved_now(w: &mut RoutingWatch) -> bool {
        tokio::select! {
            biased;
            _ = w.moved() => true,
            _ = std::future::ready(()) => false,
        }
    }

    /// CONTRACT (states.md "Routing": a stop is applied on receipt): the book's routing watch moves
    /// when a mesh status is taken, a held birth says another status, or an overlay opens; a repeat
    /// of the held mesh status moves nothing. Must NOT happen: a mesh status that changes what routing
    /// selects taken silently, so a view waits for its poll to apply it.
    #[tokio::test]
    async fn a_mesh_status_taken_moves_the_books_routing_watch() {
        let (book, fabric) = (DigestBook::default(), FabricId::parse("fab000000001").unwrap());
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        assert!(book.record(digest(&id, &inc, None, MemberStatus::ReadyForTraffic, 100)));
        let mut w = book.routing_watch();
        w.seen();
        assert!(!moved_now(&mut w).await, "nothing moved yet");
        let f = Frame::MeshStatus { mesh: "mesh1".into(), status: "leaving".into(), publisher: "mesh1.admin.1".into(), forwarded_by: None, changed_at_rafka_ms: 10 };
        assert!(book.statuses.take(&f, &fabric));
        assert!(moved_now(&mut w).await, "a mesh status taken moves the routing watch");
        w.seen();
        assert!(!book.statuses.take(&f, &fabric), "the same change again is taken once");
        assert!(!moved_now(&mut w).await, "a repeat moves nothing");
        assert!(book.record(digest(&id, &inc, None, MemberStatus::Draining, 110)));
        assert!(moved_now(&mut w).await, "a birth saying another status moves it");
    }

    #[test]
    fn a_deleting_overlay_keeps_the_node_held_and_not_routable_and_a_departure_removes_it() {
        let book = DigestBook::default();
        hold_mesh_status(&book, "mesh1", "ready-for-traffic", 10);
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

    /// The mesh-primary's report-taking of `node.started` of the birth `inc` of `id`: its
    /// DeclareNodeState(ReadyForTraffic) applied by its StatusAuthority.
    fn node_started(book: &DigestBook, id: &NodeId, inc: &IncarnationId) {
        book.started(id, inc);
    }

    /// CONTRACT (gossip.md "Node-stopped lifecycle overlay": "NodeRestarting stays until node.started
    /// of the same `(node.id, incarnation)` arrives; a digest with a different incarnation does not
    /// complete it"; node-status-heartbeat-connections.md §A.1: a heartbeat "never says started"): at
    /// the mesh-primary of mesh1, a restart holds its birth, not routable, through every heartbeat of
    /// it: its ready, Draining, Leaving, Pending and ready again heartbeats are taken as liveness and
    /// close nothing. node.started of another birth of the node closes nothing; node.started of the
    /// same birth closes it, and the birth is routable again. In a second book, a digest of a later
    /// incarnation of the node does not complete the restart of the earlier one. Must NOT happen: a
    /// heartbeat, or a digest of another incarnation, ending a restart.
    #[test]
    fn a_restart_holds_the_birth_until_node_started_of_the_same_birth() {
        let book = DigestBook::default();
        *book.own_mesh.lock().unwrap() = "mesh1".into();
        book.primary.store(true, Ordering::Relaxed);
        hold_mesh_status(&book, "mesh1", "ready-for-traffic", 10);
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        assert!(book.record(digest(&id, &inc, None, MemberStatus::ReadyForTraffic, 100)));
        let r = op(&id, &inc, "restart-node:mesh1.rpc.1");
        assert!(book.restarting(r.clone()));
        assert!(!book.restarting(r.clone()), "the same event twice is applied once");
        assert!(!book.routable(id.as_str()), "a restarting birth is not routable");
        for (status, seq) in [(MemberStatus::ReadyForTraffic, 150), (MemberStatus::Draining, 160), (MemberStatus::Leaving, 200), (MemberStatus::Pending, 300), (MemberStatus::ReadyForTraffic, 310)] {
            assert!(book.record(digest(&id, &inc, None, status, seq)), "its {status:?} heartbeat is taken as liveness");
            assert!(book.restart_of(id.as_str(), &inc).is_some(), "its {status:?} heartbeat closes nothing");
        }
        assert!(!book.routable(id.as_str()), "the birth is still restarting");
        node_started(&book, &id, &IncarnationId::mint());
        assert!(book.restart_of(id.as_str(), &inc).is_some(), "node.started of another birth closes nothing");
        node_started(&book, &id, &inc);
        assert!(book.restart_of(id.as_str(), &inc).is_none(), "node.started of the same birth closes the restart");
        assert!(book.routable(id.as_str()), "the started birth is ready-for-traffic and routable");
        assert!(book.departed().is_empty(), "a restart departs nothing");
        assert_eq!(book.get(id.as_str()).unwrap().0.node.incarnation, inc, "the book shows the same birth throughout");

        let book = DigestBook::default();
        let (id, inc, later) = (NodeId::mint(), IncarnationId::mint(), IncarnationId::mint());
        assert!(book.record(digest(&id, &inc, None, MemberStatus::ReadyForTraffic, 1)));
        assert!(book.restarting(op(&id, &inc, "restart-node:mesh1.rpc.2")));
        assert!(book.record(digest(&id, &later, Some(inc.clone()), MemberStatus::Pending, 1)), "the later incarnation's digest is taken");
        assert!(book.restart_of(id.as_str(), &inc).is_some(), "a digest of another incarnation does not complete the restart");
    }

    /// CONTRACT (gossip.md "Node-stopped lifecycle overlay": "NodeRestarting stays until node.started
    /// of the same `(node.id, incarnation)` arrives"; §3.1 `MembersDelta` carries "overlay and departed
    /// changes"; R-HB1: "NodeStopped, NodeDeleting and NodeRestarting stay immediate events"): an
    /// own-mesh member holding its seated primary's projection v1 takes the primary's `NodeRestarting`
    /// of mesh1.rpc.1 stamped v2. The birth's heartbeats (Pending, then ready) close nothing. The
    /// primary's deltas, as its forwarder derives them: v1→v2 opens the restart; v2→v3 lists the birth
    /// changed while the restart is still open, and the restart stays; v3→v4 lists the birth changed
    /// after node.started closed the restart at the primary, and the member's held projection and book
    /// both end it, ordered past the version that last carried it. Must NOT happen: a heartbeat ending
    /// a restart at a member, or a delta that cannot say a restart ended.
    #[test]
    fn an_own_mesh_member_ends_a_restart_only_when_its_primarys_projection_moves_past_it() {
        use crate::snapshot::Full;
        use tracing_subscriber::layer::SubscriberExt;
        let spans = OpenedSpans::default();
        let _capture = tracing::subscriber::set_default(tracing_subscriber::registry().with(spans.clone()));
        let fabric = FabricId::parse("fab000000001").unwrap();
        let primary = birth_at("mesh1.admin.1", 1);
        let p = publisher_of(&primary);
        let rpc = birth_at("mesh1.rpc.1", 1);
        let at = |status, seq| {
            let mut d = rpc.clone();
            d.status = status;
            d.digest_seq = seq;
            d
        };
        let restart = op_at("mesh1.rpc.1", &rpc.node.node_id, &rpc.node.incarnation, "restart-node:mesh1.rpc.1");
        let member = View::new("mesh1", "mesh1.rpc.9");
        member.book.seats.take(Seat::MeshPrimary, &seat_of(&primary, 1));
        let v1 = Full::new(vec![primary.clone(), at(MemberStatus::ReadyForTraffic, 1)], vec![], vec![]);
        let v2 = Full::new(vec![primary.clone(), at(MemberStatus::ReadyForTraffic, 1)], vec![restart.clone()], vec![]);
        let v3 = Full::new(vec![primary.clone(), at(MemberStatus::Pending, 1)], vec![restart.clone()], vec![]);
        let v4 = Full::new(vec![primary.clone(), at(MemberStatus::ReadyForTraffic, 1)], vec![], vec![]);
        let delta = |base: u64, from: &Full, to: &Full| {
            let d = from.delta_to(to);
            Frame::MembersDelta { mesh: "mesh1".into(), source_publisher: p.clone(), base_version: base, topology_version: base + 1, published_at_rafka_ms: base + 1, changed: d.changed, removed: d.removed, in_flight: d.in_flight, departed: d.departed }
        };
        member.take_snapshot(&snapshot_of("mesh1", &p, Some("mesh1.admin.1"), 1, 1, v1.digests(), v1.in_flight(), v1.departed()), &fabric, Side::MeshChannel);
        assert!(member.book.record(at(MemberStatus::ReadyForTraffic, 1)));
        member.take(&StatusEvent::Restarting.frame(restart.clone(), p.clone(), 2), &fabric, "mesh-channel");
        assert!(member.book.restart_of(rpc.node.node_id.as_str(), &rpc.node.incarnation).is_some(), "the event opens the restart");
        for (status, seq) in [(MemberStatus::Pending, 2), (MemberStatus::ReadyForTraffic, 3)] {
            assert!(member.book.record(at(status, seq)), "its {status:?} heartbeat is taken as liveness");
            assert!(member.book.restart_of(rpc.node.node_id.as_str(), &rpc.node.incarnation).is_some(), "its {status:?} heartbeat closes nothing");
        }
        member.take_snapshot(&delta(1, &v1, &v2), &fabric, Side::MeshChannel);
        member.take_snapshot(&delta(2, &v2, &v3), &fabric, Side::MeshChannel);
        assert!(member.book.restart_of(rpc.node.node_id.as_str(), &rpc.node.incarnation).is_some(), "a delta listing the birth while the restart is open at the primary closes nothing");
        assert!(member.mesh_rx.lock().unwrap().held_full("mesh1").is_some_and(|f| f.in_flight() == vec![restart.clone()]), "the held projection still carries the restart");
        member.take_snapshot(&delta(3, &v3, &v4), &fabric, Side::MeshChannel);
        assert!(member.mesh_rx.lock().unwrap().held_full("mesh1").is_some_and(|f| f.same_topology(&v4)), "the member holds what the primary holds");
        assert!(member.book.restart_of(rpc.node.node_id.as_str(), &rpc.node.incarnation).is_none(), "the projection moved past the restart: it is over");
        let ended = spans.named("rdm.mesh.membership.update.via-restart-ended-in-projection");
        assert_eq!(ended.iter().map(|f| (f["restart_carried_at"].clone(), f["topology_version"].clone())).collect::<Vec<_>>(), vec![("3".to_string(), "4".to_string())], "ended once, at v4, the restart last carried at v3: {ended:#?}");
    }

    /// CONTRACT (gossip.md "Node-stopped lifecycle overlay"; node-start.md step 2): a `NodeStopped`
    /// for the exact birth drops it from the book and keeps it unroutable; no digest of the stopped
    /// birth brings it back, whatever its status or sequence and whichever door it arrives through
    /// (a late `Draining`, an older heartbeat, a pending one with a higher sequence). Only the
    /// mesh-primary's `node.joined` ends the stop: the birth is held again `Pending`, as the same
    /// birth, and its later heartbeat moves on from there; a departure ends the stop for good.
    #[test]
    fn a_stopped_birth_leaves_the_book_and_only_node_joined_brings_it_back() {
        let book = DigestBook::default();
        hold_mesh_status(&book, "mesh1", "ready-for-traffic", 10);
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        assert!(book.record(digest(&id, &inc, None, MemberStatus::ReadyForTraffic, 100)));
        assert!(book.record(digest(&id, &inc, None, MemberStatus::Draining, 110)));
        let o = op(&id, &inc, "stop-node:mesh1.rpc.1");
        assert!(book.stopped(o.clone()));
        assert!(!book.stopped(o.clone()), "the same event twice is applied once");
        assert!(book.get(id.as_str()).is_none(), "dropped from the book");
        assert!(!book.routable(id.as_str()));
        assert_eq!(book.in_flight(), vec![o.clone()]);
        assert!(book.departed().is_empty(), "a stop departs nothing");
        for late in [digest(&id, &inc, None, MemberStatus::Draining, 111), digest(&id, &inc, None, MemberStatus::ReadyForTraffic, 105), digest(&id, &inc, None, MemberStatus::Pending, 109), digest(&id, &inc, None, MemberStatus::Pending, 120)] {
            assert!(!book.record(late.clone()), "{late:?}");
            assert!(!book.record_forwarded(late.clone()), "{late:?}");
            assert!(!book.record_topology(late.clone()), "{late:?}");
        }
        assert!(book.get(id.as_str()).is_none(), "no digest brought it back");
        assert_eq!(book.in_flight(), vec![o.clone()], "the stop stands");
        assert!(!book.joined(&op(&id, &IncarnationId::mint(), "start-node:mesh1.rpc.1")), "node.joined of another birth ends nothing");
        assert!(book.joined(&op(&id, &inc, "start-node:mesh1.rpc.1")), "the mesh-primary's node.joined ends the stop");
        let (held, _) = book.get(id.as_str()).expect("held again");
        assert_eq!((held.node.incarnation.clone(), held.status, held.digest_seq), (inc.clone(), MemberStatus::Pending, 120), "the same birth, Pending, from the latest heartbeat heard of it");
        assert!(book.in_flight().is_empty(), "node.joined closes the stop");
        assert!(!book.routable(id.as_str()), "the joined birth is Pending: a routing stop until it is ready-for-traffic");
        assert!(!book.record(digest(&id, &inc, None, MemberStatus::ReadyForTraffic, 118)), "a heartbeat older than the one it is held from is not taken");
        assert!(book.record(digest(&id, &inc, None, MemberStatus::ReadyForTraffic, 125)));
        assert!(book.routable(id.as_str()), "its ready-for-traffic restores selection");
        // A departure ends a stop for good.
        let o2 = op(&id, &inc, "stop-node:mesh1.rpc.1");
        assert!(book.stopped(LifecycleOp { attempt: 2, ..o2 }));
        assert!(book.depart(op(&id, &inc, "retire-node:mesh1.rpc.1")));
        assert!(book.in_flight().is_empty(), "the departure cleared the stop overlay");
        assert!(!book.record(digest(&id, &inc, None, MemberStatus::Pending, 400)), "a departed node never returns");
    }

    /// CONTRACT (gossip.md §3.1 `Members`; R-HB1): a non-primary node-admin of mesh1 takes its own
    /// mesh's statuses from its primary's backbone aggregate, and a stop's end only when that
    /// projection moves, in order, past the stop. Holding mesh1.rpc.1 stopped from the seated
    /// primary's event, which carries v2 (the version its stop changed the birth at): v1
    /// again (a refresh at the version held, built before the stop) is refused by name; v2 carries
    /// the stop; v3 lists neither (the primary's stop cleared before it held the birth again) and
    /// ends nothing; v2 again (older than the version held) installs nothing; v4
    /// lists the birth `Pending` and ends the stop, carried at v2; the birth is held again from the
    /// latest heartbeat of it refused while the stop stood, with that status. The heartbeat of the
    /// birth then gives liveness and nothing of its status, which is the projection's `Pending`, then its
    /// `ReadyForTraffic`. Must NOT happen: a stop ended by a projection that never carried it, by a
    /// refresh, or by a heartbeat; an own-mesh status taken from a heartbeat.
    #[test]
    fn a_non_primary_admin_ends_a_stop_only_when_its_primarys_projection_moves_past_it() {
        use tracing_subscriber::layer::SubscriberExt;
        let spans = OpenedSpans::default();
        let _capture = tracing::subscriber::set_default(tracing_subscriber::registry().with(spans.clone()));
        let fabric = FabricId::parse("fab000000001").unwrap();
        let view = View::new("mesh1", "mesh1.admin.2");
        let publisher = PublisherId { node: "mesh1.admin.1".into(), incarnation: IncarnationId::mint() };
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        let b = |status, seq| digest(&id, &inc, None, status, seq);
        let stop = op(&id, &inc, "stop-node:mesh1.rpc.1");
        let members = |version: u64, digests: Vec<MeshDigest>, in_flight: Vec<LifecycleOp>| Frame::Members {
            mesh: "mesh1".into(),
            publisher: publisher.clone(),
            forwarded_by: None,
            topology_version: version,
            published_at_rafka_ms: version,
            snapshot_id: version * 10 + in_flight.len() as u64,
            chunk_index: 0,
            chunk_count: 1,
            digests,
            in_flight,
            departed: Vec::new(),
        };
        let status = || view.book.get(id.as_str()).map(|(d, _)| d.status);
        assert!(view.book.record(b(MemberStatus::ReadyForTraffic, 1)));
        assert_eq!(status(), Some(MemberStatus::Pending), "a heartbeat gives no status: none is held until the projection gives one");
        view.take_snapshot(&members(1, vec![b(MemberStatus::ReadyForTraffic, 1)], vec![]), &fabric, Side::Backbone);
        assert_eq!(status(), Some(MemberStatus::ReadyForTraffic), "the projection's status");
        assert!(view.book.record(b(MemberStatus::Draining, 2)));
        assert_eq!(status(), Some(MemberStatus::ReadyForTraffic), "a heartbeat moves no status");

        // The event from the seated primary, carrying the version its stop changed the birth at (v2).
        view.book.seats.take(Seat::MeshPrimary, &SeatHolder { mesh: "mesh1".into(), node_id: NodeId::mint(), incarnation: publisher.incarnation.clone(), epoch: 1 });
        view.take(&StatusEvent::Stopped.frame(stop.clone(), publisher.clone(), 2), &fabric, "mesh-channel");
        assert!(view.book.get(id.as_str()).is_none() && view.book.in_flight() == vec![stop.clone()]);
        view.take_snapshot(&members(1, vec![b(MemberStatus::ReadyForTraffic, 1)], vec![]), &fabric, Side::Backbone);
        assert_eq!(view.book.in_flight(), vec![stop.clone()], "a refresh at the version held, built before the stop, ends nothing");
        view.take_snapshot(&members(2, vec![], vec![stop.clone()]), &fabric, Side::Backbone);
        view.take_snapshot(&members(3, vec![], vec![]), &fabric, Side::Backbone);
        assert_eq!(view.book.in_flight(), vec![stop.clone()], "a version listing neither the stop nor the birth ends nothing");
        view.take_snapshot(&members(2, vec![], vec![stop.clone()]), &fabric, Side::Backbone);
        assert!(!view.book.record(b(MemberStatus::Pending, 3)), "a heartbeat of the stopped birth is refused");
        view.take_snapshot(&members(4, vec![b(MemberStatus::Pending, 3)], vec![]), &fabric, Side::Backbone);
        assert!(view.book.in_flight().is_empty(), "the projection moved past the stop: it is over");
        assert_eq!(view.book.get(id.as_str()).map(|(d, _)| (d.digest_seq, d.status)), Some((3, MemberStatus::Pending)), "held again from the latest heartbeat it refused, with the projection's status");
        assert!(view.book.record(b(MemberStatus::ReadyForTraffic, 4)));
        assert_eq!(status(), Some(MemberStatus::Pending), "the projection's Pending, not the heartbeat's ReadyForTraffic");
        view.take_snapshot(&members(5, vec![b(MemberStatus::ReadyForTraffic, 4)], vec![]), &fabric, Side::Backbone);
        assert_eq!(status(), Some(MemberStatus::ReadyForTraffic));

        let refused = spans.named("rdm.mesh.membership.reject.via-stopped-birth");
        let projection: Vec<(String, String, String)> = refused.iter().filter(|f| f["door"] == "projection").map(|f| (f["held_version"].clone(), f["topology_version"].clone(), f["stop_carried_at"].clone())).collect();
        assert_eq!(projection, vec![("1".into(), "1".into(), "2".into())], "only the refresh before the stop listed the birth, older than the version the event carried: {refused:?}");
        assert!(refused.iter().any(|f| f["door"] == "heartbeat" && f["digest_seq"] == "3"), "{refused:?}");
        let ended = spans.named("rdm.mesh.membership.update.via-stop-ended-in-projection");
        assert_eq!(ended.iter().map(|f| (f["held_version"].clone(), f["topology_version"].clone(), f["stop_carried_at"].clone())).collect::<Vec<_>>(), vec![("3".to_string(), "4".to_string(), "2".to_string())], "ended once, at v4 over the v3 held, the stop carried at v2: {ended:?}");
    }

    /// CONTRACT (gossip.md §3.1 `Members`): a node's own birth is its own word. The mesh-primary's
    /// projection lists every member, the receiving node among them; the status it holds of itself
    /// stays the one it published, whether the projection arrives before its first publication (its
    /// name) or after it (its NodeId). Must NOT happen: a node taking a lagging status of itself from
    /// the projection.
    #[test]
    fn a_node_takes_no_status_of_itself_from_its_primarys_projection() {
        let fabric = FabricId::parse("fab000000001").unwrap();
        let view = View::new("mesh1", "mesh1.rpc.1");
        let publisher = PublisherId { node: "mesh1.admin.1".into(), incarnation: IncarnationId::mint() };
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        let itself = |status, seq| digest(&id, &inc, None, status, seq);
        let members = |version: u64, d: MeshDigest| Frame::Members { mesh: "mesh1".into(), publisher: publisher.clone(), forwarded_by: Some("mesh1.admin.1".into()), topology_version: version, published_at_rafka_ms: version, snapshot_id: version, chunk_index: 0, chunk_count: 1, digests: vec![d], in_flight: vec![], departed: vec![] };
        view.take_snapshot(&members(1, itself(MemberStatus::Pending, 1)), &fabric, Side::MeshChannel);
        assert!(view.book.member_statuses.lock().unwrap().get(id.as_str()).is_none(), "before its first publication, by its name");
        *view.book.own.lock().unwrap() = Some(id.to_string());
        assert!(view.book.record(itself(MemberStatus::ReadyForTraffic, 2)));
        view.take_snapshot(&members(2, itself(MemberStatus::Leaving, 1)), &fabric, Side::MeshChannel);
        assert_eq!(view.book.get(id.as_str()).map(|(d, _)| d.status), Some(MemberStatus::ReadyForTraffic), "after it, by its NodeId: its own status stands");
    }

    /// CONTRACT (gossip.md §3.1 `Members`; node-status-heartbeat-connections.md §A.3: "a delayed
    /// predecessor publication cannot switch receivers back"): the mesh-primary authors its own
    /// mesh's statuses. An aggregate of its own mesh from another publisher (a predecessor's last one,
    /// arriving after this node took the seat), on the backbone or its own channel, moves none of
    /// them. Must NOT happen: the primary's authored status overwritten by a projection copy.
    #[test]
    fn a_mesh_primary_takes_no_status_of_its_own_mesh_from_another_publishers_projection() {
        let fabric = FabricId::parse("fab000000001").unwrap();
        let view = View::new("mesh1", "mesh1.admin.2");
        view.primary.store(true, Ordering::Relaxed);
        let predecessor = PublisherId { node: "mesh1.admin.1".into(), incarnation: IncarnationId::mint() };
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        assert!(view.book.record(digest(&id, &inc, None, MemberStatus::Pending, 5)), "the primary holds its member's heartbeat");
        assert!(view.book.started(&id, &inc), "and sets it ready-for-traffic from its node.started");
        for (side, forwarded_by) in [(Side::Backbone, None), (Side::MeshChannel, Some("mesh1.admin.1".to_string()))] {
            let late = Frame::Members { mesh: "mesh1".into(), publisher: predecessor.clone(), forwarded_by, topology_version: 9, published_at_rafka_ms: 9, snapshot_id: 9, chunk_index: 0, chunk_count: 1, digests: vec![digest(&id, &inc, None, MemberStatus::Draining, 6)], in_flight: vec![], departed: vec![] };
            view.take_snapshot(&late, &fabric, side);
            assert_eq!(view.book.get(id.as_str()).map(|(d, _)| (d.status, d.digest_seq)), Some((MemberStatus::ReadyForTraffic, 5)), "{side:?}: the authored status stands");
            assert_eq!(view.book.member_statuses.lock().unwrap().get(id.as_str()).map(|(_, s)| *s), Some(MemberStatus::ReadyForTraffic));
        }
    }

    /// CONTRACT (gossip.md §3.1 `MembersDelta` carries "overlay and departed changes"; §3.3 a delta is
    /// "the complete difference between the two projections"; gossip.md: "NodeRestarting stays until
    /// node.started of the same `(node.id, incarnation)` arrives"): a delta's `changed` entry of a birth
    /// clears that exact birth's stop and restart overlays in the held projection: its source lists a
    /// stopped birth again only after node.joined cleared the stop, and carries a changed birth's open
    /// restart again beside it. A deleting overlay of the birth, and a stop of another incarnation,
    /// stay. The forwarder's delta for a joined birth carries it in `changed` and no overlay; for a
    /// birth that changed while its restart stays open, the restart again, so it stays; for one whose
    /// restart node.started ended, none, so it ends at the receiver.
    #[test]
    fn a_delta_listing_a_birth_again_clears_only_that_births_stop_and_restart_overlays() {
        use crate::snapshot::{Delta, Full};
        let (id, inc, other) = (NodeId::mint(), IncarnationId::mint(), IncarnationId::mint());
        let stop = op(&id, &inc, "stop-node:mesh1.rpc.1");
        let old_stop = LifecycleOp { build_id: "b0".into(), ..op(&id, &other, "stop-node:mesh1.rpc.1") };
        let restart = op(&id, &inc, "restart-node:mesh1.rpc.1");
        let deleting = op(&id, &inc, "retire-node:mesh1.rpc.1");
        let mut held = Full::new(Vec::new(), vec![stop.clone(), old_stop.clone(), restart.clone(), deleting.clone()], Vec::new());
        held.apply(&Delta { changed: vec![digest(&id, &inc, None, MemberStatus::Pending, 9)], ..Delta::default() });
        let mut left = held.in_flight();
        left.sort_by(|a, b| a.key().cmp(&b.key()));
        let mut want = vec![old_stop, deleting];
        want.sort_by(|a, b| a.key().cmp(&b.key()));
        assert_eq!(left, want, "only the exact birth's stop and restart overlays are cleared");
        let at_stop = Full::new(Vec::new(), vec![stop.clone()], Vec::new());
        let joined = Full::new(vec![digest(&id, &inc, None, MemberStatus::Pending, 9)], Vec::new(), Vec::new());
        let delta = at_stop.delta_to(&joined);
        assert_eq!((delta.changed.len(), delta.in_flight.len(), delta.removed.len()), (1, 0, 0), "the clear travels as the birth's own changed entry: {delta:?}");
        let mut receiver = at_stop.clone();
        receiver.apply(&delta);
        assert!(receiver.same_topology(&joined), "the receiver holds what the source holds");
        let restarting = |status| Full::new(vec![digest(&id, &inc, None, status, 9)], vec![restart.clone()], Vec::new());
        let (leaving, pending) = (restarting(MemberStatus::Leaving), restarting(MemberStatus::Pending));
        let still_open = leaving.delta_to(&pending);
        assert_eq!((still_open.changed.len(), still_open.in_flight.clone()), (1, vec![restart.clone()]), "an open restart travels again beside its changed birth");
        let mut receiver = leaving.clone();
        receiver.apply(&still_open);
        assert!(receiver.same_topology(&pending), "the open restart stays at the receiver");
        let started = Full::new(vec![digest(&id, &inc, None, MemberStatus::ReadyForTraffic, 9)], Vec::new(), Vec::new());
        let ended = pending.delta_to(&started);
        assert!(ended.in_flight.is_empty(), "a restart node.started ended does not travel");
        receiver.apply(&ended);
        assert!(receiver.same_topology(&started), "the restart ends at the receiver");
    }

    /// CONTRACT (membership.rs `restarts_from_facts`; node-restart.md: no event is inferred from
    /// gossip): the facts end a restart hold only by its own operation's `Complete` in its own Build,
    /// by its own attempt's `Converged` verdict in its own Build, or by its Build being forgotten.
    /// Must NOT happen: a hold the facts do not name yet closed by a recomputation, or closed by the
    /// same operation completing, or another attempt converging, in another Build.
    #[test]
    fn the_facts_close_a_restart_hold_only_by_its_complete_its_converged_attempt_or_its_forgotten_build() {
        let book = DigestBook::default();
        let (none, no_build): (BTreeSet<(String, String)>, BTreeSet<String>) = (BTreeSet::new(), BTreeSet::new());
        let unconverged: BTreeSet<(String, u32)> = BTreeSet::new();
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        let restart = op(&id, &inc, "restart-node:mesh1.rpc.1");
        assert!(book.restarting(restart.clone()));
        book.restarts_from_facts(&[], &none, &unconverged, &no_build);
        assert!(book.restart_of(id.as_str(), &inc).is_some(), "facts that do not name the hold yet leave it open");
        let elsewhere: BTreeSet<(String, String)> = [("b2".to_string(), restart.operation.clone())].into();
        book.restarts_from_facts(&[], &elsewhere, &unconverged, &no_build);
        assert!(book.restart_of(id.as_str(), &inc).is_some(), "the same operation completing in another Build is not its Complete");
        let own: BTreeSet<(String, String)> = [(restart.build_id.clone(), restart.operation.clone())].into();
        book.restarts_from_facts(&[], &own, &unconverged, &no_build);
        assert!(book.restart_of(id.as_str(), &inc).is_none(), "its own Complete closes it");

        let (id2, inc2) = (NodeId::mint(), IncarnationId::mint());
        let forgotten_one = LifecycleOp { build_id: "b3".into(), ..op(&id2, &inc2, "restart-node:mesh1.rpc.2") };
        assert!(book.restarting(forgotten_one.clone()));
        book.restarts_from_facts(&[], &none, &unconverged, &["b1".to_string()].into());
        assert!(book.restart_of(id2.as_str(), &inc2).is_some(), "another Build forgotten leaves it open");
        book.restarts_from_facts(&[], &none, &unconverged, &["b3".to_string()].into());
        assert!(book.restart_of(id2.as_str(), &inc2).is_none(), "its Build forgotten closes it");

        let (id3, inc3) = (NodeId::mint(), IncarnationId::mint());
        let converging = LifecycleOp { attempt: 3, ..op(&id3, &inc3, "restart-node:mesh1.rpc.3") };
        assert!(book.restarting(converging.clone()));
        book.restarts_from_facts(&[], &none, &[(converging.build_id.clone(), 4), ("b2".to_string(), 3)].into(), &no_build);
        assert!(book.restart_of(id3.as_str(), &inc3).is_some(), "another attempt converging, or its attempt in another Build, leaves it open");
        book.restarts_from_facts(&[], &none, &[(converging.build_id.clone(), 3)].into(), &no_build);
        assert!(book.restart_of(id3.as_str(), &inc3).is_none(), "its own attempt Converged closes it");
    }

    /// CONTRACT (gossip.md §3.1: a Digest "carries no status: a member's status comes only from its
    /// mesh-primary"; states.md "Who sets each state": node ReadyForTraffic is set by "the node's
    /// mesh-primary" from "the node's node.started report"): at the mesh-primary of mesh1, a member's
    /// heartbeats move no status, whatever the member's own word. Its ready-for-traffic heartbeat
    /// before node.started leaves it Pending, unroutable, and its projection entry Pending;
    /// node.started sets it ready-for-traffic; a later heartbeat in which it says Draining, held from
    /// memory or decoded from the wire, leaves it ready-for-traffic. Must NOT happen: the primary
    /// setting a member's status from its heartbeat.
    #[test]
    fn a_members_heartbeat_moves_no_status_at_its_mesh_primary() {
        let book = DigestBook::default();
        *book.own_mesh.lock().unwrap() = "mesh1".into();
        book.primary.store(true, Ordering::Relaxed);
        hold_mesh_status(&book, "mesh1", "ready-for-traffic", 10);
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        let status = |book: &DigestBook| book.get(id.as_str()).map(|(d, _)| d.status);
        assert!(book.record(digest(&id, &inc, None, MemberStatus::ReadyForTraffic, 100)), "the heartbeat is taken as liveness");
        assert_eq!(status(&book), Some(MemberStatus::Pending), "a ready-for-traffic heartbeat before node.started sets nothing");
        assert!(!book.routable(id.as_str()), "the birth is Pending: not routable");
        assert_eq!(book.current(book.staleness_floor()).into_iter().find(|d| d.node.node_id == id).map(|d| d.status), Some(MemberStatus::Pending), "its projection entry is Pending");
        node_started(&book, &id, &inc);
        assert_eq!(status(&book), Some(MemberStatus::ReadyForTraffic), "node.started sets it ready-for-traffic");
        assert!(book.record(digest(&id, &inc, None, MemberStatus::Draining, 110)), "the heartbeat is taken as liveness");
        assert_eq!(status(&book), Some(MemberStatus::ReadyForTraffic), "a heartbeat saying Draining moves nothing");
        let Frame::Digest { digest: heard } = Frame::decode(&Frame::Digest { digest: digest(&id, &inc, None, MemberStatus::Draining, 120) }.encode()).unwrap() else { unreachable!() };
        assert!(book.record(heard), "the heartbeat from the wire is taken as liveness");
        assert_eq!(status(&book), Some(MemberStatus::ReadyForTraffic), "nor does one from the wire");
        assert!(book.routable(id.as_str()));
    }

    /// CONTRACT (states.md "Who sets each state": node pending is set by "the node's mesh-primary"
    /// from "the node's node.joined report on the start call"; gossip.md: "NodeRestarting stays until
    /// node.started of the same `(node.id, incarnation)` arrives"): at the mesh-primary of mesh1, a
    /// restart's start sets its birth Pending at node.joined though no stop mark is held (a restart
    /// publishes no NodeStopped). The restart stays open and the birth unroutable until node.started
    /// of the same birth, which sets it ready-for-traffic and closes the restart. A node.joined of a
    /// birth this book holds nothing of sets nothing. Must NOT happen: node.joined of a restarting
    /// birth leaving it ready-for-traffic, or closing its restart.
    #[test]
    fn a_restarts_node_joined_sets_its_birth_pending_with_no_stop_held() {
        let book = DigestBook::default();
        *book.own_mesh.lock().unwrap() = "mesh1".into();
        book.primary.store(true, Ordering::Relaxed);
        hold_mesh_status(&book, "mesh1", "ready-for-traffic", 10);
        use tracing_subscriber::layer::SubscriberExt;
        let spans = OpenedSpans::default();
        let _capture = tracing::subscriber::set_default(tracing_subscriber::registry().with(spans.clone()));
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        assert!(book.record(digest(&id, &inc, None, MemberStatus::Pending, 100)));
        node_started(&book, &id, &inc);
        assert!(book.routable(id.as_str()), "the started birth is ready-for-traffic");
        let restart = op(&id, &inc, "restart-node:mesh1.rpc.1");
        assert!(book.restarting(restart.clone()));
        assert!(!book.joined(&op(&NodeId::mint(), &IncarnationId::mint(), "restart-node:mesh1.rpc.9")), "node.joined of a birth this book holds nothing of sets nothing");
        assert!(book.joined(&op(&id, &inc, "restart-node:mesh1.rpc.1")), "the restart's node.joined sets its birth Pending");
        assert_eq!(book.get(id.as_str()).map(|(d, _)| d.status), Some(MemberStatus::Pending), "held Pending from node.joined");
        assert_eq!(book.member_statuses.lock().unwrap().get(id.as_str()).map(|(i, s)| (i.clone(), *s)), Some((inc.clone(), MemberStatus::Pending)), "the status the projection carries is Pending");
        assert!(book.restart_of(id.as_str(), &inc).is_some(), "node.joined closes no restart");
        assert!(!book.routable(id.as_str()), "a restarting, Pending birth is not routable");
        let joined = spans.named("rdm.mesh.membership.update.via-node-joined");
        assert_eq!(joined.iter().map(|f| (f["status"].clone(), f["stops_cleared"].clone())).collect::<Vec<_>>(), vec![("Pending".to_string(), String::new())], "one node.joined, Pending, no stop cleared: {joined:#?}");
        node_started(&book, &id, &inc);
        assert!(book.restart_of(id.as_str(), &inc).is_none(), "node.started of the same birth closes the restart");
        assert!(book.routable(id.as_str()), "and the birth is ready-for-traffic again");
    }

    /// CONTRACT: an overlay that ended stays ended: a restart node.started of its birth ended, the
    /// primary re-applying the operation each round or the Build facts still naming it open (its
    /// `Complete` is not receipted yet), and a stop its mesh-primary's node.joined ended, re-sent by a
    /// snapshot built before it, open nothing. Must NOT happen: a started node held restarting, or a
    /// joined one stopped, again.
    #[test]
    fn an_ended_restart_or_stop_overlay_is_not_opened_again_by_a_later_copy() {
        let book = DigestBook::default();
        *book.own_mesh.lock().unwrap() = "mesh1".into();
        hold_mesh_status(&book, "mesh1", "ready-for-traffic", 10);
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        assert!(book.record(digest(&id, &inc, None, MemberStatus::ReadyForTraffic, 100)));
        let restart = op(&id, &inc, "restart-node:mesh1.rpc.1");
        assert!(book.restarting(restart.clone()));
        assert!(book.record(digest(&id, &inc, None, MemberStatus::Draining, 110)));
        assert!(book.record(digest(&id, &inc, None, MemberStatus::Pending, 120)));
        assert!(book.started(&id, &inc), "node.started of the birth");
        assert!(book.restart_of(id.as_str(), &inc).is_none());
        assert!(!book.restarting(restart.clone()), "the same operation again opens nothing");
        let (none, unconverged, no_build): (BTreeSet<(String, String)>, BTreeSet<(String, u32)>, BTreeSet<String>) = (BTreeSet::new(), BTreeSet::new(), BTreeSet::new());
        book.restarts_from_facts(&[restart], &none, &unconverged, &no_build);
        assert!(book.restart_of(id.as_str(), &inc).is_none(), "facts still naming it open, its Complete not receipted, open nothing");
        assert!(book.record(digest(&id, &inc, None, MemberStatus::ReadyForTraffic, 125)));
        assert!(book.routable(id.as_str()), "no overlay holds it: the ready birth of a ready mesh is selected");
        let stop = op(&id, &inc, "stop-node:mesh1.rpc.1");
        assert!(book.stopped(stop.clone()));
        assert!(!book.record(digest(&id, &inc, None, MemberStatus::Pending, 130)), "a heartbeat ends no stop");
        assert!(book.joined(&op(&id, &inc, "start-node:mesh1.rpc.1")));
        assert!(!book.stopped(stop), "a snapshot built before node.joined does not stop the node again");
        assert!(book.get(id.as_str()).is_some());
    }

    /// CONTRACT: a stop of an older birth than the one held, and a stop of a departed node, are refused;
    /// a node this book holds nothing of is marked stopped all the same (a late joiner reads it from
    /// the aggregate's open overlays) and refuses every heartbeat of that birth.
    #[test]
    fn a_stop_of_another_birth_is_refused_and_one_for_an_unheld_node_still_parks_it() {
        let book = DigestBook::default();
        let (id, old, new) = (NodeId::mint(), IncarnationId::mint(), IncarnationId::mint());
        assert!(book.record(digest(&id, &new, Some(old.clone()), MemberStatus::ReadyForTraffic, 100)));
        assert!(!book.stopped(op(&id, &old, "stop-node:mesh1.rpc.1")), "the stop names a birth the held one superseded");
        assert!(book.get(id.as_str()).is_some());
        let late_joiner = DigestBook::default();
        assert!(late_joiner.stopped(op(&id, &new, "stop-node:mesh1.rpc.1")));
        assert!(!late_joiner.record(digest(&id, &new, None, MemberStatus::Draining, 90)));
        assert!(!late_joiner.record(digest(&id, &new, None, MemberStatus::Pending, 91)), "a heartbeat ends no stop");
        assert!(late_joiner.joined(&op(&id, &new, "start-node:mesh1.rpc.1")), "node.joined ends it");
        assert_eq!(late_joiner.get(id.as_str()).map(|(d, _)| (d.status, d.digest_seq)), Some((MemberStatus::Pending, 91)), "held again from the latest heartbeat it refused");
    }

    /// CONTRACT: a stop is an open overlay of its mesh's aggregate (`Members.in_flight`), recognised
    /// by its operation, so a node that never heard the frame reads the node as parked from the aggregate.
    #[test]
    fn a_stop_overlay_rides_the_aggregate_and_is_told_from_a_restart_by_its_operation() {
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        let book = DigestBook::default();
        assert!(book.record(digest(&id, &inc, None, MemberStatus::ReadyForTraffic, 100)));
        let stop = op(&id, &inc, "stop-node:mesh1.rpc.1");
        assert!(stop.is_stop() && !stop.is_restart());
        assert!(!op(&id, &inc, "restart-node:mesh1.rpc.1").is_stop());
        assert!(!op(&id, &inc, "retire-node:mesh1.rpc.1").is_stop());
        assert!(book.stopped(stop.clone()));
        let (in_flight, departed) = book.overlays_of("mesh1");
        assert_eq!(in_flight, vec![stop]);
        assert!(departed.is_empty());
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

    /// CONTRACT: every digest the book takes ticks `digest_changes`, a fresher digest of the same
    /// birth and status included (a renamed node's next digest is one); a digest the book refuses
    /// as older does not tick.
    #[test]
    fn every_taken_digest_ticks_digest_changes_and_a_refused_one_does_not() {
        let book = DigestBook::default();
        let mut digests = book.digest_changes();
        let (id, birth) = (NodeId::mint(), IncarnationId::mint());
        assert!(book.record(digest(&id, &birth, None, MemberStatus::ReadyForTraffic, 100)));
        assert!(digests.has_changed().unwrap(), "the first digest ticks");
        digests.borrow_and_update();
        let mut renamed = digest(&id, &birth, None, MemberStatus::ReadyForTraffic, 101);
        renamed.config.insert("k".into(), "v".into());
        assert!(book.record(renamed));
        assert!(digests.has_changed().unwrap(), "a fresher digest of the same birth and status ticks");
        digests.borrow_and_update();
        assert!(!book.record(digest(&id, &birth, None, MemberStatus::ReadyForTraffic, 100)));
        assert!(!digests.has_changed().unwrap(), "a refused older digest does not tick");
        assert!(book.record_forwarded(digest(&id, &birth, None, MemberStatus::ReadyForTraffic, 102)));
        assert!(digests.has_changed().unwrap(), "a forwarded fresher digest ticks");
        digests.borrow_and_update();
        assert!(book.record_topology(digest(&id, &birth, None, MemberStatus::ReadyForTraffic, 103)));
        assert!(digests.has_changed().unwrap(), "a topology copy that moved the held digest ticks");
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

    /// CONTRACT (R-G2, gossip.md §3.2 and §6, mesh-investigation.md step 2): a node-admin judges a
    /// peer mesh's member from direct backbone receipt, and a member held from a backbone aggregate
    /// ages from that receipt at the shared staleness floor, exactly as a member heard on its own
    /// mesh channel: no grace is added for it. Its silence is the real time since the receipt.
    #[test]
    fn a_member_held_from_a_backbone_aggregate_ages_at_the_staleness_floor() {
        let book = DigestBook::default();
        let floor = staleness_floor();
        let (direct, backbone) = (NodeId::mint(), NodeId::mint());
        let t0 = Instant::now();
        book.record_at(digest(&direct, &IncarnationId::mint(), None, MemberStatus::ReadyForTraffic, 100), t0);
        assert!(book.record_forwarded_at(digest(&backbone, &IncarnationId::mint(), None, MemberStatus::ReadyForTraffic, 100), t0));
        let within = t0 + floor;
        assert_eq!(book.current_at(floor, within).len(), 2, "both are heard within the staleness floor");
        let past = t0 + floor + Duration::from_millis(1);
        let heard: Vec<String> = book.current_at(floor, past).into_iter().map(|d| d.node.node_id.to_string()).collect();
        assert!(heard.is_empty(), "past the staleness floor neither the direct nor the backbone-held member is heard: {heard:?}");
        assert_eq!(book.get_at(backbone.as_str(), past).unwrap().1, floor + Duration::from_millis(1), "its silence is the time since its backbone receipt");
        let silent: Vec<Duration> = book.silent_held(past).into_iter().map(|(_, s)| s).collect();
        assert_eq!(silent, vec![floor + Duration::from_millis(1); 2], "every held member's silence is measured from its receipt");
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
    /// it as well (its mesh primary's backbone `Members`) leaves it held as heard directly: it is
    /// among the members that checked in on its own channel (`heard_direct_since`), and a peer
    /// mesh's member in the same kind of aggregate is held as heard on the backbone.
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

    /// CONTRACT (i143 R-J1 follow-up, ops-naming): a member whose restart is open is held through
    /// it, but it is a parked birth that answers nobody and gossips nothing: the repair refeed
    /// never hands its address to the channel again, however long the member has been silent. Its
    /// heartbeat closes nothing; node.started of the same birth closes the restart and the member is a
    /// target again at its address.
    #[test]
    fn a_member_under_an_open_restart_is_never_a_repair_target() {
        let book = DigestBook::default();
        *book.own_mesh.lock().unwrap() = "mesh1".into();
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        let key = iroh::SecretKey::generate().public().to_string();
        let long_ago = Instant::now().checked_sub(Duration::from_secs(600)).expect("the clock has run ten minutes");
        let mut d = digest(&id, &inc, None, MemberStatus::ReadyForTraffic, 100);
        d.node.endpoint_id = EndpointId(key.clone());
        assert!(book.record_at(d.clone(), long_ago));
        assert_eq!(held_targets(&book, |_| true).len(), 1, "a silent held member is a repair target");
        assert!(book.restarting(op(&id, &inc, "restart-node:mesh1.rpc.1")));
        assert!(held_targets(&book, |_| true).is_empty(), "its restart is open: its address is never handed to the channel");
        let mut next = digest(&id, &inc, None, MemberStatus::Pending, 300);
        next.node.endpoint_id = EndpointId(key);
        assert!(book.record_at(next, long_ago));
        assert!(held_targets(&book, |_| true).is_empty(), "its heartbeat closes nothing");
        assert!(book.started(&id, &inc));
        let targets = held_targets(&book, |_| true);
        assert_eq!(targets.len(), 1, "node.started of the same birth closes the restart");
        assert_eq!(targets[0].addr.ip_addrs().next().map(|a| a.port()), Some(41000), "and is a target at its own address");
    }

    fn born(mesh: &str, kind_ordinal: &str, mesh_id: Option<&MeshId>, at: u64) -> MeshDigest {
        let mut d = digest(&NodeId::mint(), &IncarnationId::mint(), None, MemberStatus::ReadyForTraffic, at);
        d.node.name = format!("{mesh}.{kind_ordinal}").parse().unwrap();
        d.mesh_id = mesh_id.cloned();
        d
    }

    /// CONTRACT: `mesh-left` departs every birth the mesh held, through the departure a node's own
    /// stop takes: the births leave the active topology and are held departed for the departed
    /// retention and not a moment longer; nothing is kept of the mesh;
    /// hearing it again departs nothing a second time.
    #[test]
    fn mesh_left_departs_every_birth_for_the_departed_retention_and_keeps_nothing() {
        let book = DigestBook::with_floor(Duration::from_millis(50), Duration::from_secs(3));
        let m1 = MeshId::mint();
        let (admin, rpc) = (born("mesh1", "admin.1", Some(&m1), 1), born("mesh1", "rpc.1", None, 1));
        for d in [&admin, &rpc] {
            assert!(book.record_forwarded(d.clone()));
        }
        let applied = book.mesh_left(&m1, None, "bld_1", 1, 9);
        assert_eq!((applied.name.as_deref(), applied.departed), (Some("mesh1"), 2));
        assert!(book.all().is_empty(), "the births left the active topology");
        assert_eq!(book.mesh_left(&m1, None, "bld_1", 1, 9).departed, 0, "idempotent");
        assert!(book.is_departed(admin.node.node_id.as_str()) && book.is_departed(rpc.node.node_id.as_str()));
        assert!(book.departed().iter().all(|op| op.operation.starts_with("stop-node:mesh1.") && op.build_id == "bld_1"));
        // Within the retention a late digest of a departed birth is refused.
        let mut late = admin.clone();
        late.digest_seq += 10;
        assert!(!book.record(late.clone()) && !book.record_forwarded(late.clone()) && !book.record_topology(late));
        std::thread::sleep(Duration::from_millis(80));
        assert!(book.departed().is_empty(), "the departures are held for the normal retention only");
    }

    /// CONTRACT (R4, Luke 2026-10-11; lifecycles/mesh-leave.md "Every stopping member publishes its
    /// own Leaving … carries that same Leaving digest in its final `Members` aggregate over the
    /// node-admin backbone channel"): a peer mesh learns that a leaving mesh's members are leaving
    /// only from each member's own state. A node that joins a peer mesh after those members said
    /// Draining and Leaving installs their own states from its mesh primary's topology read (op
    /// `0x1E`) and forwarder replay, and selects none of them for new work; a member still saying
    /// ReadyForTraffic is routed by that state alone, with no mesh-level hold.
    #[test]
    fn a_node_joining_after_the_leaving_meshs_members_said_draining_and_leaving_installs_their_own_states_and_routes_none_of_them() {
        let fabric = FabricId::parse("fab000000001").unwrap();
        let m1 = MeshId::mint();
        let (admin, draining, leaving) = (born("mesh1", "admin.1", Some(&m1), 1), born("mesh1", "rpc.1", None, 1), born("mesh1", "rpc.2", None, 1));
        let publisher = PublisherId { node: "mesh1.admin.1".into(), incarnation: admin.node.incarnation.clone() };
        let members = |version: u64, digests: Vec<MeshDigest>| Frame::Members {
            mesh: "mesh1".into(),
            publisher: publisher.clone(),
            forwarded_by: None,
            topology_version: version,
            published_at_rafka_ms: version,
            snapshot_id: version,
            chunk_index: 0,
            chunk_count: 1,
            digests,
            in_flight: Vec::new(),
            departed: Vec::new(),
        };
        let said = |d: &MeshDigest, status: MemberStatus| {
            let mut d = d.clone();
            d.status = status;
            d.digest_seq += 1;
            d.emitted_at_rafka_ms += 1;
            d
        };
        // mesh2's primary holds mesh1 and forwards it into mesh2; mesh-leaving is heard; then the
        // leaving primary's next aggregate carries its members' own Draining and Leaving.
        let primary = View::new("mesh2", "mesh2.admin.1");
        primary.primary.store(true, Ordering::Relaxed);
        // mesh1's primary published mesh1 ready-for-traffic before the leave; the joiner learns it
        // from the replay.
        primary.take(&Frame::MeshStatus { mesh: "mesh1".into(), status: "ready-for-traffic".into(), publisher: "mesh1.admin.1".into(), forwarded_by: None, changed_at_rafka_ms: 5 }, &fabric, "backbone");
        for (version, digests) in [(1, vec![admin.clone(), draining.clone(), leaving.clone()]), (2, vec![admin.clone(), said(&draining, MemberStatus::Draining), said(&leaving, MemberStatus::Leaving)])] {
            let taken = primary.take_snapshot(&members(version, digests), &fabric, Side::Backbone);
            let (mesh, from, v, full) = taken.source.expect("a complete source snapshot");
            let _ = primary.forwarder.lock().unwrap().source("mesh2.admin.1", &mesh, &from, v, &full, version);
            if version == 1 {
                primary.take(&Frame::MeshLeaving { mesh_id: m1.clone(), build_id: "bld_1".into(), attempt: 1, operation: format!("shutdown-mesh:{m1}"), publisher: "mesh3.admin.1".into(), event_at_rafka_ms: 7, forwarded_by: None }, &fabric, "backbone");
            }
        }
        // A new mesh2 node joins: it reads the primary's topology (0x1E), then hears its replay.
        let joiner = View::new("mesh2", "mesh2.rpc.9");
        for s in primary.forwarder.lock().unwrap().snapshots() {
            for f in crate::snapshot::chunks_of(&s.full(), |digests, in_flight, departed, chunk_index, chunk_count| Frame::Members {
                mesh: s.mesh.clone(),
                publisher: s.publisher.clone(),
                forwarded_by: None,
                topology_version: s.topology_version,
                published_at_rafka_ms: 0,
                snapshot_id: 1,
                chunk_index,
                chunk_count,
                digests,
                in_flight,
                departed,
            }) {
                let Frame::Members { mesh, publisher, topology_version, snapshot_id, chunk_index, chunk_count, digests, in_flight, departed, .. } = f else { unreachable!() };
                let chunk = crate::snapshot::Chunk { mesh, publisher, forwarded_by: None, topology_version, snapshot_id, chunk_index, chunk_count, digests, in_flight, departed };
                let prior = Prior::of(&joiner.mesh_rx.lock().unwrap(), &chunk.mesh);
                let installed = joiner.mesh_rx.lock().unwrap().take_read_chunk(chunk);
                if let crate::snapshot::Taken::Installed(i) = installed {
                    joiner.installed(&i, &fabric, Side::Read, 0, prior);
                }
            }
        }
        let mut replay = primary.book.statuses.held_frames(&fabric, "mesh2.admin.1");
        replay.extend(primary.book.seats.held_frames());
        replay.extend(primary.forwarder.lock().unwrap().replay("mesh2.admin.1", 3));
        for f in &replay {
            match f {
                Frame::Members { .. } | Frame::MembersDelta { .. } => {
                    joiner.take_snapshot(f, &fabric, Side::MeshChannel);
                }
                _ => {
                    joiner.take(f, &fabric, "mesh-channel");
                }
            }
        }
        let status = |d: &MeshDigest| joiner.book.get(d.node.node_id.as_str()).map(|(h, _)| h.status);
        assert_eq!(
            (status(&admin), status(&draining), status(&leaving)),
            (Some(MemberStatus::ReadyForTraffic), Some(MemberStatus::Draining), Some(MemberStatus::Leaving)),
            "the joiner installed each member at its address with the member's own state"
        );
        assert!(!joiner.book.routable(draining.node.node_id.as_str()), "a member saying Draining takes no new work");
        assert!(!joiner.book.routable(leaving.node.node_id.as_str()), "a member saying Leaving takes no new work");
        assert!(joiner.book.routable(admin.node.node_id.as_str()), "a member still saying ReadyForTraffic, of a mesh the joiner holds ready-for-traffic, is selected");
    }

    /// CONTRACT (gossip.md, "Snapshot, delta and top-up agreement": after mesh-left the mesh is absent
    /// from the projection): a node of a peer mesh that joins after `mesh-left` installs nothing of
    /// the departed mesh from its primary's topology read or neighbour-up replay, so it can route
    /// nothing to the mesh's births.
    #[test]
    fn a_node_joining_after_mesh_left_installs_nothing_of_the_departed_mesh() {
        let fabric = FabricId::parse("fab000000001").unwrap();
        let m1 = MeshId::mint();
        let (admin, rpc) = (born("mesh1", "admin.1", Some(&m1), 1), born("mesh1", "rpc.1", None, 1));
        let publisher = PublisherId { node: "mesh1.admin.1".into(), incarnation: IncarnationId::mint() };
        let members = Frame::Members {
            mesh: "mesh1".into(),
            publisher: publisher.clone(),
            forwarded_by: None,
            topology_version: 1,
            published_at_rafka_ms: 1,
            snapshot_id: 1,
            chunk_index: 0,
            chunk_count: 1,
            digests: vec![admin.clone(), rpc.clone()],
            in_flight: Vec::new(),
            departed: Vec::new(),
        };
        let primary = View::new("mesh2", "mesh2.admin.1");
        primary.primary.store(true, Ordering::Relaxed);
        let taken = primary.take_snapshot(&members, &fabric, Side::Backbone);
        let (mesh, from, version, full) = taken.source.expect("a complete source snapshot");
        let _ = primary.forwarder.lock().unwrap().source("mesh2.admin.1", &mesh, &from, version, &full, 1);
        let op = format!("shutdown-mesh:{m1}");
        primary.take(&Frame::MeshLeaving { mesh_id: m1.clone(), build_id: "bld_1".into(), attempt: 1, operation: op.clone(), publisher: "mesh3.admin.1".into(), event_at_rafka_ms: 7, forwarded_by: None }, &fabric, "backbone");
        primary.take(&Frame::MeshLeft { mesh_id: m1.clone(), build_id: "bld_1".into(), attempt: 1, operation: op, receipt_manifest: "m".into(), publisher: "mesh3.admin.1".into(), event_at_rafka_ms: 9, forwarded_by: None }, &fabric, "backbone");
        assert!(primary.forwarder.lock().unwrap().snapshots().iter().all(|s| s.mesh != "mesh1"), "the primary serves no topology of the departed mesh");
        let joiner = View::new("mesh2", "mesh2.rpc.9");
        let mut replay = primary.book.statuses.held_frames(&fabric, "mesh2.admin.1");
        replay.extend(primary.book.seats.held_frames());
        replay.extend(primary.forwarder.lock().unwrap().replay("mesh2.admin.1", 2));
        for f in &replay {
            match f {
                Frame::Members { .. } | Frame::MembersDelta { .. } => {
                    joiner.take_snapshot(f, &fabric, Side::MeshChannel);
                }
                _ => {
                    joiner.take(f, &fabric, "mesh-channel");
                }
            }
        }
        assert!(joiner.book.all().iter().all(|d| d.node.name.mesh != "mesh1"), "the joiner holds no birth of the departed mesh");
        assert!(!joiner.book.routable(admin.node.node_id.as_str()) && !joiner.book.routable(rpc.node.node_id.as_str()), "the joiner routes nothing to the departed mesh's births");
    }

    /// CONTRACT (gossip.md, snapshot, delta and top-up agreement): a Leaving mesh is still updated
    /// by the source projection this node holds; after `mesh-left` the source projection is gone,
    /// the accepted Build does not name the mesh, and an older aggregate of it, the same version
    /// again or a newer one, installs nothing.
    #[test]
    fn an_old_aggregate_of_a_mesh_the_build_no_longer_names_installs_nothing() {
        let view = View::new("mesh2", "mesh2.rpc.1");
        let fabric = FabricId::parse("fab000000001").unwrap();
        let m1 = MeshId::mint();
        let (admin, rpc) = (born("mesh1", "admin.1", Some(&m1), 1), born("mesh1", "rpc.1", None, 1));
        let publisher = PublisherId { node: "mesh1.admin.1".into(), incarnation: IncarnationId::mint() };
        let members = |version: u64, snapshot_id: u64| Frame::Members {
            mesh: "mesh1".into(),
            publisher: publisher.clone(),
            forwarded_by: None,
            topology_version: version,
            published_at_rafka_ms: 1,
            snapshot_id,
            chunk_index: 0,
            chunk_count: 1,
            digests: vec![admin.clone(), rpc.clone()],
            in_flight: Vec::new(),
            departed: Vec::new(),
        };
        view.book.set_accepted_meshes(["mesh1".to_string(), "mesh2".to_string()].into());
        let taken = view.take_snapshot(&members(1, 1), &fabric, Side::Backbone);
        assert_eq!(taken.carried.len(), 2, "the mesh is held before it leaves");
        // The Build now names {mesh2}: mesh1 is leaving, and the source this node holds is updated.
        view.book.set_accepted_meshes(["mesh2".to_string()].into());
        view.take(&Frame::MeshLeaving { mesh_id: m1.clone(), build_id: "bld_1".into(), attempt: 1, operation: format!("shutdown-mesh:{m1}"), publisher: "mesh3.admin.1".into(), event_at_rafka_ms: 7, forwarded_by: None }, &fabric, "backbone");
        let leaving = view.take_snapshot(&members(2, 2), &fabric, Side::Backbone);
        assert!(!leaving.carried.is_empty(), "a leaving mesh's source is still held and still moves");
        view.take(&Frame::MeshLeft { mesh_id: m1.clone(), build_id: "bld_1".into(), attempt: 1, operation: format!("shutdown-mesh:{m1}"), receipt_manifest: "m".into(), publisher: "mesh3.admin.1".into(), event_at_rafka_ms: 9, forwarded_by: None }, &fabric, "backbone");
        assert!(view.book.all().iter().all(|d| d.node.name.mesh != "mesh1"), "its births left the active topology");
        assert!(view.backbone_rx.lock().unwrap().held_full("mesh1").is_none() && view.mesh_rx.lock().unwrap().held_full("mesh1").is_none(), "no source projection of the mesh is kept");
        assert!(view.forwarder.lock().unwrap().replay("mesh2.admin.1", 1).is_empty(), "nothing of the mesh is replayed");
        // The same version again, and a newer one: neither installs anything.
        for (v, id) in [(1, 3), (2, 4), (3, 5)] {
            let again = view.take_snapshot(&members(v, id), &fabric, Side::Backbone);
            assert!(again.carried.is_empty(), "version {v}: no birth of the mesh is taken");
            assert!(view.backbone_rx.lock().unwrap().held_full("mesh1").is_none(), "version {v}: no source projection of the mesh is installed");
        }
        assert!(view.book.all().iter().all(|d| d.node.name.mesh != "mesh1"));
    }

    /// Two node-admins of mesh1, both Ready: `(lower NodeId, higher NodeId)`, named admin.2 and
    /// admin.1 so the seat can sit on either.
    fn two_admins(book: &DigestBook, at: Instant) -> (MeshDigest, MeshDigest) {
        let (mut a, mut b) = (NodeId::mint(), NodeId::mint());
        if b < a {
            std::mem::swap(&mut a, &mut b);
        }
        let mut lower = digest(&a, &IncarnationId::mint(), None, MemberStatus::ReadyForTraffic, 10);
        lower.node.name = "mesh1.admin.2".parse().unwrap();
        let mut higher = digest(&b, &IncarnationId::mint(), None, MemberStatus::ReadyForTraffic, 10);
        higher.node.name = "mesh1.admin.1".parse().unwrap();
        assert!(book.record_at(lower.clone(), at) && book.record_at(higher.clone(), at));
        (lower, higher)
    }

    fn seat_of(d: &MeshDigest, epoch: u64) -> SeatHolder {
        SeatHolder { mesh: d.node.name.mesh.clone(), node_id: d.node.node_id.clone(), incarnation: d.node.incarnation.clone(), epoch }
    }

    /// CONTRACT (gossip.md §3.3, R-A2): a member tops up from its own mesh's current primary, the
    /// birth its held seat record names, never from an election of its own. With the seat on the
    /// admin whose NodeId is the higher of two Ready admins, the member's primary is that holder.
    #[test]
    fn a_members_mesh_primary_is_the_seat_holder_not_the_lowest_ready_admin() {
        let book = DigestBook::default();
        let (_lower, holder) = two_admins(&book, Instant::now());
        book.seats.take(Seat::MeshPrimary, &seat_of(&holder, 3));
        assert_eq!(book.mesh_primary("mesh1").ok().map(|d| d.node.name.to_string()), Some("mesh1.admin.1".into()));
    }

    /// CONTRACT (R-EL1): a holder that is Draining keeps its seat, so the member still tops up from it.
    #[test]
    fn a_draining_seat_holder_is_still_the_members_mesh_primary() {
        let book = DigestBook::default();
        let (lower, holder) = two_admins(&book, Instant::now());
        book.seats.take(Seat::MeshPrimary, &seat_of(&lower, 3));
        let mut draining = lower.clone();
        draining.status = MemberStatus::Draining;
        draining.digest_seq += 1;
        draining.emitted_at_rafka_ms += 1;
        assert!(book.record(draining));
        let _ = holder;
        assert_eq!(book.mesh_primary("mesh1").ok().map(|d| d.node.name.to_string()), Some("mesh1.admin.2".into()));
    }

    /// CONTRACT: the seat holder's digest aged past the staleness floor does not move the seat:
    /// only a new record or a proven loss does, so the member still addresses the holder.
    #[test]
    fn a_seat_holder_unheard_past_the_staleness_floor_is_still_the_members_mesh_primary() {
        let book = DigestBook::default();
        let long_ago = Instant::now().checked_sub(Duration::from_secs(600)).expect("the clock has run ten minutes");
        let (lower, _higher) = two_admins(&book, long_ago);
        book.seats.take(Seat::MeshPrimary, &seat_of(&lower, 3));
        assert_eq!(book.mesh_primary("mesh1").ok().map(|d| d.node.name.to_string()), Some("mesh1.admin.2".into()));
    }

    /// CONTRACT: a node that cannot address its mesh's primary says which fact stops it, with the
    /// record it holds: no record of its mesh, a record naming a birth it holds no digest of (or a
    /// later birth of that node), and a record naming a birth held as proven gone.
    #[test]
    fn a_mesh_primary_that_cannot_be_addressed_names_why() {
        let book = DigestBook::default();
        assert_eq!(book.mesh_primary("mesh1").unwrap_err(), NoMeshPrimary::NoSeatRecord { mesh: "mesh1".into() });
        let unheard = SeatHolder { mesh: "mesh1".into(), node_id: NodeId::mint(), incarnation: IncarnationId::mint(), epoch: 1 };
        book.seats.take(Seat::MeshPrimary, &unheard);
        assert_eq!(book.mesh_primary("mesh1").unwrap_err(), NoMeshPrimary::HolderNotHeld { holder: unheard, held_incarnation: None, departed: false });

        let book = DigestBook::default();
        let (lower, _) = two_admins(&book, Instant::now());
        let mut earlier = seat_of(&lower, 2);
        earlier.incarnation = IncarnationId::mint();
        book.seats.take(Seat::MeshPrimary, &earlier);
        let err = book.mesh_primary("mesh1").unwrap_err();
        assert_eq!((err.reason(), err.holder()), ("seat-holder-not-held", Some(&earlier)));
        assert_eq!(err, NoMeshPrimary::HolderNotHeld { holder: earlier, held_incarnation: Some(lower.node.incarnation.clone()), departed: false });
        let held = seat_of(&lower, 3);
        book.seats.take(Seat::MeshPrimary, &held);
        assert_eq!(book.mesh_primary("mesh1").map(|d| d.node.node_id), Ok(lower.node.node_id.clone()));
        book.seats.mark_gone(held.incarnation.clone());
        assert_eq!(book.mesh_primary("mesh1").unwrap_err(), NoMeshPrimary::HolderGone { holder: held });
    }

    /// Every span opened while the capture is the thread's subscriber: its name and its fields.
    #[derive(Clone, Default)]
    struct OpenedSpans(Arc<Mutex<Vec<(String, BTreeMap<String, String>)>>>);

    struct SpanFields<'a>(&'a mut BTreeMap<String, String>);

    impl tracing::field::Visit for SpanFields<'_> {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            self.0.insert(field.name().to_string(), format!("{value:?}").trim_matches('"').to_string());
        }
    }

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for OpenedSpans {
        fn on_new_span(&self, attrs: &tracing::span::Attributes<'_>, _id: &tracing::span::Id, _ctx: tracing_subscriber::layer::Context<'_, S>) {
            let mut fields = BTreeMap::new();
            attrs.record(&mut SpanFields(&mut fields));
            self.0.lock().unwrap().push((attrs.metadata().name().to_string(), fields));
        }
    }

    impl OpenedSpans {
        fn named(&self, name: &str) -> Vec<BTreeMap<String, String>> {
            self.0.lock().unwrap().iter().filter(|(n, _)| n == name).map(|(_, f)| f.clone()).collect()
        }
    }

    fn op_at(path: &str, id: &NodeId, inc: &IncarnationId, operation: &str) -> LifecycleOp {
        LifecycleOp { name: path.parse().unwrap(), ..op(id, inc, operation) }
    }

    /// CONTRACT (R-NS1): the fabric primary lists, for a mesh beyond its own, exactly the stops and
    /// deletes it executed: a stop it published from the matching stopped reply while the stop is
    /// open, a delete it published from the exit proof while the departure is retained. A stop
    /// another admin executed, and its own mesh's overlays, are never listed; a stop that ended
    /// lists nothing again.
    #[test]
    fn the_fabric_primary_lists_for_a_mesh_beyond_its_own_only_the_stops_and_deletes_it_executed() {
        let book = DigestBook::default();
        let (last, other, own, deleted) = ((NodeId::mint(), IncarnationId::mint()), (NodeId::mint(), IncarnationId::mint()), (NodeId::mint(), IncarnationId::mint()), (NodeId::mint(), IncarnationId::mint()));
        let stop_last = op_at("mesh1.admin.1", &last.0, &last.1, "stop-node:mesh1.admin.1");
        let stop_other = op_at("mesh1.rpc.1", &other.0, &other.1, "stop-node:mesh1.rpc.1");
        let stop_own = op_at("mesh2.rpc.1", &own.0, &own.1, "stop-node:mesh2.rpc.1");
        let delete = op_at("mesh4.admin.1", &deleted.0, &deleted.1, "retire-node:mesh4.admin.1");
        for o in [&stop_last, &stop_other, &stop_own] {
            assert!(book.stopped(o.clone()));
        }
        assert!(book.depart(delete.clone()));
        assert!(book.executed_overlays_beyond("mesh2").is_empty(), "nothing executed, nothing listed");
        book.executed_stop(&stop_last);
        book.executed_stop(&stop_own);
        book.executed_delete(&delete);
        let listed = book.executed_overlays_beyond("mesh2");
        assert_eq!(listed.keys().cloned().collect::<Vec<_>>(), ["mesh1", "mesh4"], "{listed:?}");
        assert_eq!(listed["mesh1"], (vec![stop_last.clone()], vec![]), "only the stop it executed, never mesh1's other stop");
        assert_eq!(listed["mesh4"], (vec![], vec![delete.clone()]));
        // The last admin is started again: its stop ends at node.joined and it is listed no more.
        assert!(book.joined(&LifecycleOp { operation: "start-node:mesh1.admin.1".into(), ..stop_last.clone() }));
        assert!(!book.executed_overlays_beyond("mesh2").contains_key("mesh1"), "a start hands publishing back");
    }

    /// CONTRACT (R-NS1 change 3): a NodeStopped arriving after its birth's mesh left, and a
    /// NodeDeleted other than the departure held, are refused by the departed fence by name and
    /// resurrect nothing; a copy of the departure held (every aggregate carries it for the retention)
    /// is a copy, not a refusal.
    #[test]
    fn a_lifecycle_overlay_arriving_after_its_mesh_left_is_refused_by_the_departed_fence_by_name() {
        use tracing_subscriber::layer::SubscriberExt;
        let spans = OpenedSpans::default();
        let _capture = tracing::subscriber::set_default(tracing_subscriber::registry().with(spans.clone()));
        let book = DigestBook::default();
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        let mesh_id = MeshId::mint();
        let mut d = digest(&id, &inc, None, MemberStatus::Leaving, 100);
        d.node.name = "mesh1.admin.1".parse().unwrap();
        d.mesh_id = Some(mesh_id.clone());
        assert!(book.record(d));
        assert_eq!(book.mesh_left(&mesh_id, Some("mesh1"), "b1", 1, 1).departed, 1);
        let late_stop = op_at("mesh1.admin.1", &id, &inc, "stop-node:mesh1.admin.1");
        let late_stop = LifecycleOp { build_id: "b0".into(), ..late_stop };
        assert!(!book.stopped(late_stop.clone()), "a late NodeStopped opens nothing");
        assert!(book.in_flight().is_empty() && book.get(id.as_str()).is_none(), "nothing is resurrected");
        let late_delete = op_at("mesh1.admin.1", &id, &inc, "retire-node:mesh1.admin.1");
        assert!(!book.depart(late_delete.clone()), "a late NodeDeleted departs nothing again");
        let held = book.departed().into_iter().next().unwrap();
        assert!(!book.depart(held), "a copy of the departure held");
        let refused = spans.named("rdm.mesh.membership.reject.via-departed-birth");
        assert_eq!(refused.len(), 2, "one refusal per late frame, none for the copy: {refused:?}");
        assert_eq!((refused[0]["frame"].as_str(), refused[0]["operation"].as_str(), refused[0]["build_id"].as_str()), ("node-stopped", "stop-node:mesh1.admin.1", "b0"));
        assert_eq!((refused[1]["frame"].as_str(), refused[1]["operation"].as_str()), ("node-deleted", "retire-node:mesh1.admin.1"));
        assert!(refused.iter().all(|f| f["node_id"] == id.to_string() && f["incarnation_id"] == inc.0));
    }

    /// CONTRACT (R-NS1 change 3): a departure refused by the departed fence is refused by name once.
    /// Its publisher carries it in every aggregate for the retention (it never held the departure this
    /// book holds), so each later copy of the same refused departure is a copy of a refusal already
    /// made: refused again, opening no second span. Must NOT happen: one refusal span per aggregate
    /// copy (canonical_ids: 44 refusals of one replace-node departure at one admin).
    #[test]
    fn a_departure_refused_by_the_departed_fence_is_refused_by_name_once_and_its_copies_silently() {
        use tracing_subscriber::layer::SubscriberExt;
        let spans = OpenedSpans::default();
        let _capture = tracing::subscriber::set_default(tracing_subscriber::registry().with(spans.clone()));
        let book = DigestBook::default();
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        let mesh_id = MeshId::mint();
        let mut d = digest(&id, &inc, None, MemberStatus::Leaving, 100);
        d.node.name = "mesh2.admin.2".parse().unwrap();
        d.mesh_id = Some(mesh_id.clone());
        assert!(book.record(d));
        assert_eq!(book.mesh_left(&mesh_id, Some("mesh2"), "b1", 1, 1).departed, 1);
        let replaced = LifecycleOp { build_id: "b2".into(), attempt: 2, ..op_at("mesh2.admin.2", &id, &inc, "replace-node:mesh2.admin.2") };
        for copy in 0..3 {
            assert!(!book.depart(replaced.clone()), "copy {copy} of the refused departure departs nothing again");
        }
        let refused = spans.named("rdm.mesh.membership.reject.via-departed-birth");
        assert_eq!(refused.len(), 1, "one refusal by name, its copies silent: {refused:?}");
        assert_eq!((refused[0]["frame"].as_str(), refused[0]["operation"].as_str(), refused[0]["build_id"].as_str()), ("node-deleted", "replace-node:mesh2.admin.2", "b2"));
        // Another departure of the same birth is a fact of its own: refused by name once.
        let retired = op_at("mesh2.admin.2", &id, &inc, "retire-node:mesh2.admin.2");
        assert!(!book.depart(retired.clone()) && !book.depart(retired));
        assert_eq!(spans.named("rdm.mesh.membership.reject.via-departed-birth").len(), 2, "each refused departure once");
    }

    /// A birth of `path` with a fresh NodeId and incarnation, ready, heartbeat `seq`.
    fn birth_at(path: &str, seq: u64) -> MeshDigest {
        let mut d = digest(&NodeId::mint(), &IncarnationId::mint(), None, MemberStatus::ReadyForTraffic, seq);
        d.node.name = path.parse().unwrap();
        d
    }

    /// One complete snapshot of `mesh` by `publisher` at `version`, as `side` carries it.
    fn snapshot_of(mesh: &str, publisher: &PublisherId, forwarded_by: Option<&str>, version: u64, snapshot_id: u64, digests: Vec<MeshDigest>, in_flight: Vec<LifecycleOp>, departed: Vec<LifecycleOp>) -> Frame {
        Frame::Members { mesh: mesh.into(), publisher: publisher.clone(), forwarded_by: forwarded_by.map(String::from), topology_version: version, published_at_rafka_ms: version, snapshot_id, chunk_index: 0, chunk_count: 1, digests, in_flight, departed }
    }

    fn publisher_of(d: &MeshDigest) -> PublisherId {
        PublisherId { node: d.node.name.to_string(), incarnation: d.node.incarnation.clone() }
    }

    /// CONTRACT (node-status-heartbeat-connections.md §A.3: "a delayed predecessor publication cannot
    /// switch receivers back"; gossip.md §3.3: "the source publisher's exact identity is its epoch";
    /// §A.4: "A stopped birth publishes nothing"): once a receiver installed a source's next epoch, a
    /// delayed publication of the epoch before it is refused by name and the receiver keeps the newer
    /// epoch. Three predecessors, each told by what the receiver holds:
    /// - the acting fabric-primary's aggregate of mesh2 (its primary deleted, held departed from that
    ///   aggregate) is installed on the backbone; mesh2.admin.1's delayed last aggregate (snapshot 3)
    ///   is refused, naming the publisher departed (rafka-v2 s41's red, trace
    ///   6cf5759abdce42d61c414560c772b308);
    /// - mesh3's acting aggregate carries its last admin's stop; that admin's delayed aggregate is
    ///   refused, naming the publisher stopped;
    /// - on an ordinary node's mesh channel, mesh1.admin.2 took mesh1's seat (epoch 2) and its full is
    ///   installed over mesh1.admin.1's; mesh1.admin.1's delayed full and its delayed delta are each
    ///   refused, naming the seat holder; the unknown epoch marks the source for GetTopology repair
    ///   (gossip.md §3.1) and the held projection stays the successor's.
    /// Must NOT happen: a receiver holding an earlier publisher's snapshot again after the next
    /// epoch's, or a predecessor's delta read as a version gap.
    #[test]
    fn a_delayed_predecessor_publication_is_refused_by_name_and_never_switches_the_receiver_back() {
        use tracing_subscriber::layer::SubscriberExt;
        let spans = OpenedSpans::default();
        let _capture = tracing::subscriber::set_default(tracing_subscriber::registry().with(spans.clone()));
        let fabric = FabricId::parse("fab000000001").unwrap();
        let fp = PublisherId { node: "mesh1.admin.1".into(), incarnation: IncarnationId::mint() };
        let held = |view: &View, side: Side, mesh: &str| view.receiver(side).lock().unwrap().held_version(mesh);

        // The backbone: a peer mesh primary holds mesh2 and mesh3 from their own primaries.
        let peer = View::new("mesh4", "mesh4.admin.1");
        let (admin2, rpc) = (birth_at("mesh2.admin.1", 1), [birth_at("mesh2.rpc.1", 1), birth_at("mesh2.rpc.2", 1)]);
        let admin3 = birth_at("mesh3.admin.1", 1);
        let mesh2_full = || vec![admin2.clone(), rpc[0].clone(), rpc[1].clone()];
        peer.take_snapshot(&snapshot_of("mesh2", &publisher_of(&admin2), None, 1, 1, mesh2_full(), vec![], vec![]), &fabric, Side::Backbone);
        peer.take_snapshot(&snapshot_of("mesh3", &publisher_of(&admin3), None, 1, 1, vec![admin3.clone()], vec![], vec![]), &fabric, Side::Backbone);
        // The fabric-primary, acting for both: mesh2.admin.1 deleted, mesh3.admin.1 stopped.
        let deleted = op_at("mesh2.admin.1", &admin2.node.node_id, &admin2.node.incarnation, "delete-node:mesh2.admin.1");
        let stopped = op_at("mesh3.admin.1", &admin3.node.node_id, &admin3.node.incarnation, "stop-node:mesh3.admin.1");
        peer.take_snapshot(&snapshot_of("mesh2", &fp, None, 1, 1, rpc.to_vec(), vec![], vec![deleted.clone()]), &fabric, Side::Backbone);
        peer.take_snapshot(&snapshot_of("mesh3", &fp, None, 1, 2, vec![], vec![stopped.clone()], vec![]), &fabric, Side::Backbone);
        assert_eq!(held(&peer, Side::Backbone, "mesh2"), Some((fp.clone(), 1)), "the acting aggregate of mesh2 is installed");
        assert_eq!(held(&peer, Side::Backbone, "mesh3"), Some((fp.clone(), 1)), "the acting aggregate of mesh3 is installed");
        // The predecessors' delayed aggregates.
        peer.take_snapshot(&snapshot_of("mesh2", &publisher_of(&admin2), None, 1, 3, mesh2_full(), vec![], vec![]), &fabric, Side::Backbone);
        peer.take_snapshot(&snapshot_of("mesh3", &publisher_of(&admin3), None, 1, 3, vec![admin3.clone()], vec![], vec![]), &fabric, Side::Backbone);
        assert_eq!(held(&peer, Side::Backbone, "mesh2"), Some((fp.clone(), 1)), "mesh2.admin.1's delayed aggregate does not switch the peer back");
        assert_eq!(held(&peer, Side::Backbone, "mesh3"), Some((fp.clone(), 1)), "mesh3.admin.1's delayed aggregate does not switch the peer back");
        assert!(peer.book.is_departed(admin2.node.node_id.as_str()) && peer.book.get(admin2.node.node_id.as_str()).is_none(), "the departed primary is not held again");

        // An ordinary node's mesh channel: mesh1's seat moves from mesh1.admin.1 to mesh1.admin.2.
        let member = View::new("mesh1", "mesh1.rpc.9");
        let (p1, p2, rpc1) = (birth_at("mesh1.admin.1", 1), birth_at("mesh1.admin.2", 1), birth_at("mesh1.rpc.1", 1));
        assert!(matches!(member.book.seats.take(Seat::MeshPrimary, &seat_of(&p1, 1)), SeatTaken::Held { .. }));
        member.take_snapshot(&snapshot_of("mesh1", &publisher_of(&p1), Some("mesh1.admin.1"), 5, 1, vec![p1.clone(), p2.clone(), rpc1.clone()], vec![], vec![]), &fabric, Side::MeshChannel);
        assert!(matches!(member.book.seats.take(Seat::MeshPrimary, &seat_of(&p2, 2)), SeatTaken::Held { .. }));
        member.take_snapshot(&snapshot_of("mesh1", &publisher_of(&p2), Some("mesh1.admin.2"), 1, 1, vec![p1.clone(), p2.clone(), rpc1.clone()], vec![], vec![]), &fabric, Side::MeshChannel);
        assert_eq!(held(&member, Side::MeshChannel, "mesh1"), Some((publisher_of(&p2), 1)), "the successor's full is installed: its epoch");
        member.take_snapshot(&snapshot_of("mesh1", &publisher_of(&p1), Some("mesh1.admin.1"), 6, 2, vec![p1.clone(), rpc1.clone()], vec![], vec![]), &fabric, Side::MeshChannel);
        assert_eq!(held(&member, Side::MeshChannel, "mesh1"), Some((publisher_of(&p2), 1)), "the predecessor's delayed full does not switch the member back");
        let late_delta = Frame::MembersDelta { mesh: "mesh1".into(), source_publisher: publisher_of(&p1), base_version: 5, topology_version: 6, published_at_rafka_ms: 6, changed: vec![], removed: vec![p2.node.node_id.to_string()], in_flight: vec![], departed: vec![] };
        member.take_snapshot(&late_delta, &fabric, Side::MeshChannel);
        assert_eq!(held(&member, Side::MeshChannel, "mesh1"), Some((publisher_of(&p2), 1)));
        assert_eq!(
            member.mesh_rx.lock().unwrap().desynced().iter().map(|(m, g)| (m.clone(), g.to_string())).collect::<Vec<_>>(),
            vec![("mesh1".to_string(), "unknown-epoch".to_string())],
            "a predecessor's publication is refused, not read as a version gap: its unknown epoch marks the source for GetTopology repair from the member's own primary"
        );

        let refused = spans.named("rdm.mesh.membership.reject.via-predecessor-publisher");
        let said: Vec<(String, String, String, String)> = refused.iter().map(|f| (f["node"].clone(), f["mesh"].clone(), f["frame"].clone(), f["reason"].clone())).collect();
        assert_eq!(
            said,
            vec![
                ("mesh4.admin.1".to_string(), "mesh2".to_string(), "members".to_string(), "publisher-departed".to_string()),
                ("mesh4.admin.1".to_string(), "mesh3".to_string(), "members".to_string(), "publisher-stopped".to_string()),
                ("mesh1.rpc.9".to_string(), "mesh1".to_string(), "members".to_string(), "not-seated".to_string()),
                ("mesh1.rpc.9".to_string(), "mesh1".to_string(), "members-delta".to_string(), "not-seated".to_string()),
            ],
            "each delayed predecessor publication is refused by name: {refused:#?}"
        );
        assert!(refused.iter().all(|f| !f["publisher"].is_empty() && !f["held_publisher"].is_empty()), "{refused:#?}");
        assert_eq!(refused[2]["seat_holder"], seat_of(&p2, 2).to_string(), "the refusal names the seat holder: {refused:#?}");
    }

    /// CONTRACT (gossip.md §3.1 `Members`/`MembersDelta`: "Missing versions or an unknown source
    /// epoch trigger GetTopology repair"; §3.3 the top-up): an ordinary node of mesh2 holds mesh1's
    /// seat record of mesh1.admin.1's lost birth (from its entry read: no `Seated` reaches a peer
    /// mesh's channel) and mesh1's forwarded projection of that birth. mesh1 recovers as itself and
    /// its new primary birth publishes; mesh2's primary forwards the new epoch's full and then a
    /// delta. Each is refused by name (the receiver holds no seat evidence for that publisher), and
    /// the source is marked for GetTopology repair (`unknown-epoch`), so the node's top-up reads its
    /// own primary, whose read installs the new epoch. Must NOT happen: the node refusing the new
    /// epoch with nothing to repair it, held on the lost birth's projection for good (mesh_recover
    /// `a_lost_mesh_recovers_as_itself_under_the_same_build`: mesh2.rpc.1-3 refused 35 frames each).
    #[test]
    fn a_publication_of_an_epoch_with_no_seat_evidence_on_the_mesh_channel_is_refused_and_marked_for_top_up() {
        use tracing_subscriber::layer::SubscriberExt;
        let spans = OpenedSpans::default();
        let _capture = tracing::subscriber::set_default(tracing_subscriber::registry().with(spans.clone()));
        let fabric = FabricId::parse("fab000000001").unwrap();
        let member = View::new("mesh2", "mesh2.rpc.1");
        let (lost, recovered, rpc) = (birth_at("mesh1.admin.1", 1), birth_at("mesh1.admin.1", 1), birth_at("mesh1.rpc.1", 1));
        assert!(matches!(member.book.seats.take(Seat::MeshPrimary, &seat_of(&lost, 1)), SeatTaken::Held { .. }), "the entry read's seat record of mesh1");
        member.take_snapshot(&snapshot_of("mesh1", &publisher_of(&lost), Some("mesh2.admin.1"), 7, 1, vec![lost.clone(), rpc.clone()], vec![], vec![]), &fabric, Side::MeshChannel);
        assert_eq!(member.mesh_rx.lock().unwrap().held_version("mesh1"), Some((publisher_of(&lost), 7)));
        member.take_snapshot(&snapshot_of("mesh1", &publisher_of(&recovered), Some("mesh2.admin.1"), 1, 2, vec![recovered.clone(), rpc.clone()], vec![], vec![]), &fabric, Side::MeshChannel);
        let delta = Frame::MembersDelta { mesh: "mesh1".into(), source_publisher: publisher_of(&recovered), base_version: 1, topology_version: 2, published_at_rafka_ms: 2, changed: vec![], removed: vec![rpc.node.node_id.to_string()], in_flight: vec![], departed: vec![] };
        member.take_snapshot(&delta, &fabric, Side::MeshChannel);
        assert_eq!(member.mesh_rx.lock().unwrap().held_version("mesh1"), Some((publisher_of(&lost), 7)), "nothing of an epoch without seat evidence is installed from the channel");
        let desynced = member.mesh_rx.lock().unwrap().desynced();
        assert_eq!(desynced.iter().map(|(m, g)| (m.clone(), g.to_string())).collect::<Vec<_>>(), vec![("mesh1".to_string(), "unknown-epoch".to_string())], "the unknown epoch marks the source for GetTopology repair");
        let refused = spans.named("rdm.mesh.membership.reject.via-predecessor-publisher");
        assert_eq!(refused.iter().map(|f| (f["frame"].clone(), f["reason"].clone(), f["repair"].clone())).collect::<Vec<_>>(), vec![("members".to_string(), "not-seated".to_string(), "get-topology".to_string()), ("members-delta".to_string(), "not-seated".to_string(), "get-topology".to_string())], "{refused:#?}");
    }

    /// CONTRACT (node-status-heartbeat-connections.md §C "status before seat evidence": "a status fact
    /// from a publisher the receiver does not yet hold as seated is refused by name (nothing is held
    /// for later); the top-up supplies the full"; R-HB1 G1): a `NodeStopped`, `NodeRestarting` or
    /// `NodeDeleting` is taken only from the birth the held seat record of the subject's mesh names, or
    /// from the held fabric-primary (the acting primary of a mesh with no primary). On an own-mesh
    /// member's mesh channel: a publisher the seat record does not name is refused naming the holder,
    /// and the source is marked for GetTopology repair (an unknown source epoch, gossip.md §3.1); with no
    /// seat record held the event is refused naming that, and nothing is marked (the node's first
    /// topology read is its top-up). A publisher held stopped is refused naming it stopped. The seated
    /// primary's event, and the fabric-primary's, are taken. Must NOT happen: a status event of a
    /// publisher without seat evidence applied, or held for later.
    #[test]
    fn a_status_event_from_a_publisher_the_receiver_does_not_hold_seated_is_refused_by_name() {
        use tracing_subscriber::layer::SubscriberExt;
        let spans = OpenedSpans::default();
        let _capture = tracing::subscriber::set_default(tracing_subscriber::registry().with(spans.clone()));
        let fabric = FabricId::parse("fab000000001").unwrap();
        let (primary, other, fabric_primary, stopped_admin) = (birth_at("mesh1.admin.1", 1), birth_at("mesh1.admin.2", 1), birth_at("mesh2.admin.1", 1), birth_at("mesh1.admin.3", 1));
        let subject = |path: &str| birth_at(path, 1);
        let event = |kind: StatusEvent, s: &MeshDigest, operation: &str, by: &MeshDigest| kind.frame(op_at(&s.node.name.to_string(), &s.node.node_id, &s.node.incarnation, operation), publisher_of(by), 4);

        // No seat record held: refused, nothing marked.
        let member = View::new("mesh1", "mesh1.rpc.9");
        let s1 = subject("mesh1.rpc.1");
        member.take(&event(StatusEvent::Stopped, &s1, "stop-node:mesh1.rpc.1", &primary), &fabric, "mesh-channel");
        assert!(member.book.in_flight().is_empty(), "no seat evidence: the stop is not held");
        assert!(member.mesh_rx.lock().unwrap().desynced().is_empty(), "no seat record: nothing is marked for repair");

        // The seat record names mesh1.admin.1: mesh1.admin.2's events are refused and mark the source.
        assert!(matches!(member.book.seats.take(Seat::MeshPrimary, &seat_of(&primary, 1)), SeatTaken::Held { .. }));
        member.take_snapshot(&snapshot_of("mesh1", &publisher_of(&primary), Some("mesh1.admin.1"), 3, 1, vec![primary.clone(), s1.clone()], vec![], vec![]), &fabric, Side::MeshChannel);
        for kind in [StatusEvent::Stopped, StatusEvent::Restarting, StatusEvent::Deleting] {
            member.take(&event(kind, &s1, "x-node:mesh1.rpc.1", &other), &fabric, "mesh-channel");
        }
        assert!(member.book.in_flight().is_empty(), "a publisher the seat record does not name moves nothing");
        assert_eq!(member.mesh_rx.lock().unwrap().desynced().iter().map(|(m, g)| (m.clone(), g.to_string())).collect::<Vec<_>>(), vec![("mesh1".to_string(), "unknown-epoch".to_string())], "the unknown epoch marks the source for GetTopology repair");

        // A publisher held stopped publishes nothing: its delayed event is refused.
        let mesh2_member = View::new("mesh1", "mesh1.rpc.8");
        mesh2_member.book.seats.take(Seat::MeshPrimary, &seat_of(&stopped_admin, 1));
        assert!(mesh2_member.book.stopped(op_at("mesh1.admin.3", &stopped_admin.node.node_id, &stopped_admin.node.incarnation, "stop-node:mesh1.admin.3")));
        let s2 = subject("mesh1.rpc.2");
        mesh2_member.take(&event(StatusEvent::Deleting, &s2, "delete-node:mesh1.rpc.2", &stopped_admin), &fabric, "mesh-channel");
        assert_eq!(mesh2_member.book.in_flight().len(), 1, "only the publisher's own stop is held");

        // The seated primary's event is taken; the held fabric-primary's (acting for the mesh) too.
        let taker = View::new("mesh1", "mesh1.rpc.7");
        taker.book.seats.take(Seat::MeshPrimary, &seat_of(&primary, 1));
        taker.book.seats.take(Seat::FabricPrimary, &seat_of(&fabric_primary, 1));
        let (s3, s4) = (subject("mesh1.rpc.3"), subject("mesh1.rpc.4"));
        taker.take(&event(StatusEvent::Stopped, &s3, "stop-node:mesh1.rpc.3", &primary), &fabric, "mesh-channel");
        taker.take(&event(StatusEvent::Restarting, &s4, "restart-node:mesh1.rpc.4", &fabric_primary), &fabric, "mesh-channel");
        assert_eq!(taker.book.in_flight().len(), 2, "the seated primary's and the fabric-primary's events are taken: {:?}", taker.book.in_flight());

        let refused: Vec<(String, String, String, String)> = spans.named("rdm.mesh.membership.reject.via-unseated-publisher").iter().map(|f| (f["frame"].clone(), f["reason"].clone(), f["repair"].clone(), f["seat_holder"].clone())).collect();
        let holder = seat_of(&primary, 1).to_string();
        assert_eq!(
            refused,
            vec![
                ("node-stopped".to_string(), "no-seat-record".to_string(), "none".to_string(), String::new()),
                ("node-stopped".to_string(), "not-seated".to_string(), "get-topology".to_string(), holder.clone()),
                ("node-restarting".to_string(), "not-seated".to_string(), "get-topology".to_string(), holder.clone()),
                ("node-deleting".to_string(), "not-seated".to_string(), "get-topology".to_string(), holder),
                ("node-deleting".to_string(), "publisher-stopped".to_string(), "none".to_string(), String::new()),
            ],
            "each refusal names its reason"
        );
    }

    /// CONTRACT (node-status-heartbeat-connections.md §C "late stopped after starting": "a NodeStopped
    /// older than the starting fact (lower `topology_version`) is refused"; §A.2 "Receivers compare per
    /// subject"; R-HB1 G1: "A receiver orders a fact only against facts of the same publisher epoch"):
    /// a member holds its seated primary's projection v5 listing mesh1.rpc.1 again after its start. The
    /// primary's `NodeStopped` of that birth stamped v4 arrives late: it is refused by name, naming the
    /// held version, and the birth stays held. The same event stamped v6 (after the held projection) is
    /// taken; so is a v4 stop from another seated publisher's epoch (the fabric-primary's), which is
    /// never ordered against this projection. Must NOT happen: a late stop dropping a started birth.
    #[test]
    fn a_node_stopped_older_than_the_held_projection_of_its_publisher_listing_the_birth_is_refused_by_name() {
        use tracing_subscriber::layer::SubscriberExt;
        let spans = OpenedSpans::default();
        let _capture = tracing::subscriber::set_default(tracing_subscriber::registry().with(spans.clone()));
        let fabric = FabricId::parse("fab000000001").unwrap();
        let (primary, fabric_primary) = (birth_at("mesh1.admin.1", 1), birth_at("mesh2.admin.1", 1));
        let rpc = birth_at("mesh1.rpc.1", 3);
        let stop = op_at("mesh1.rpc.1", &rpc.node.node_id, &rpc.node.incarnation, "stop-node:mesh1.rpc.1");
        let member = |name: &str| {
            let v = View::new("mesh1", name);
            v.book.seats.take(Seat::MeshPrimary, &seat_of(&primary, 1));
            v.book.seats.take(Seat::FabricPrimary, &seat_of(&fabric_primary, 1));
            v.take_snapshot(&snapshot_of("mesh1", &publisher_of(&primary), Some("mesh1.admin.1"), 5, 1, vec![primary.clone(), rpc.clone()], vec![], vec![]), &fabric, Side::MeshChannel);
            assert!(v.book.record(rpc.clone()));
            v
        };
        let late = member("mesh1.rpc.7");
        late.take(&StatusEvent::Stopped.frame(stop.clone(), publisher_of(&primary), 4), &fabric, "mesh-channel");
        assert!(late.book.in_flight().is_empty() && late.book.get(rpc.node.node_id.as_str()).is_some(), "a stop older than the projection listing the birth is refused; the birth stays held");
        let newer = member("mesh1.rpc.8");
        newer.take(&StatusEvent::Stopped.frame(stop.clone(), publisher_of(&primary), 6), &fabric, "mesh-channel");
        assert_eq!(newer.book.in_flight(), vec![stop.clone()], "a stop newer than the held projection is taken");
        let other_epoch = member("mesh1.rpc.9");
        other_epoch.take(&StatusEvent::Stopped.frame(stop.clone(), publisher_of(&fabric_primary), 4), &fabric, "mesh-channel");
        assert_eq!(other_epoch.book.in_flight(), vec![stop.clone()], "another publisher's epoch is never ordered against this projection");
        let refused = spans.named("rdm.mesh.membership.reject.via-stop-older-than-projection");
        assert_eq!(refused.iter().map(|f| (f["node"].clone(), f["topology_version"].clone(), f["held_version"].clone(), f["held_channel"].clone())).collect::<Vec<_>>(), vec![("mesh1.rpc.7".to_string(), "4".to_string(), "5".to_string(), "mesh-channel".to_string())], "{refused:#?}");
    }

    /// Two receive paths of one node (its backbone and its mesh channel) applying a projection of
    /// the stopped birth `d` at once, each having found the stop held and the projection past it.
    fn both_paths_end_the_stop(book: &DigestBook, d: &MeshDigest, fence: &Fence) {
        for _ in 0..2 {
            book.stopped_entry(d, fence);
        }
    }

    /// CONTRACT (gossip.md "Node-stopped lifecycle overlay": the stop stays "until the next lifecycle
    /// op on that exact birth"; node_stop gate 4679736d, mesh1.admin.2: the stop of mesh1.rpc.1 ended
    /// twice, trace 09b25fab91c3239df30466a89f81d507 on the mesh channel and
    /// 4b4eac62071c2a5054e464c5f3a429af on the backbone, the second naming `stop_carried_at=-1`): a
    /// node-admin applies its primary's projection on its backbone and on its mesh channel, each under
    /// its own receiver. When both find the stop held and the projection past it, the stop ends once:
    /// one span, carrying the version the stop was carried at, and the birth held again once, from
    /// the latest heartbeat refused while the stop stood. Must NOT happen: a second end of a stop
    /// already ended, naming no carried version.
    #[test]
    fn a_stop_two_receive_paths_find_moved_past_at_once_ends_once() {
        use tracing_subscriber::layer::SubscriberExt;
        let spans = OpenedSpans::default();
        let _capture = tracing::subscriber::set_default(tracing_subscriber::registry().with(spans.clone()));
        let view = View::new("mesh1", "mesh1.admin.2");
        let primary = birth_at("mesh1.admin.1", 1);
        view.book.seats.take(Seat::MeshPrimary, &seat_of(&primary, 1));
        let rpc = birth_at("mesh1.rpc.1", 5);
        let stop = op_at("mesh1.rpc.1", &rpc.node.node_id, &rpc.node.incarnation, "stop-node:mesh1.rpc.1");
        assert!(view.book.record(rpc.clone()));
        view.take(&StatusEvent::Stopped.frame(stop.clone(), publisher_of(&primary), 11), &FabricId::parse("fab000000001").unwrap(), "mesh-channel");
        let mut late = rpc.clone();
        late.digest_seq = 6;
        assert!(!view.book.record(late), "the stopped birth's heartbeat is refused and kept as the latest");
        let mut listed = rpc.clone();
        listed.status = MemberStatus::Pending;
        let fence = Fence { publisher: publisher_of(&primary).to_string(), channel: "mesh-channel", held: Some((publisher_of(&primary).to_string(), 11)), version: 12, ..Fence::default() };
        both_paths_end_the_stop(&view.book, &listed, &fence);
        assert!(view.book.in_flight().is_empty(), "the stop is over");
        let ended = spans.named("rdm.mesh.membership.update.via-stop-ended-in-projection");
        assert_eq!(ended.iter().map(|f| (f["stop_carried_at"].clone(), f["held_again"].clone(), f["held_digest_seq"].clone())).collect::<Vec<_>>(), vec![("11".to_string(), "true".to_string(), "6".to_string())], "ended once: {ended:#?}");
    }

    /// CONTRACT (gossip.md §3.1 `MembersDelta` carries "overlay and departed changes"; §3.3 a delta is
    /// "the complete difference between the two projections"; node_restart gate 6c64abe1: mesh1.rpc.1
    /// and mesh1.rpc.3 ended mesh1.rpc.2's restart at v15, two versions after the primary's node.started
    /// closed it at v13, because the delta 12→14 carried no entry of mesh1.rpc.2, whose digest had not
    /// changed): a restart that ended at the source while its birth's digest stayed the same still
    /// ends at the receiver: the delta names the birth whose restart ended. Must NOT happen: a
    /// receiver's held projection keeping a restart its source no longer carries.
    #[test]
    fn a_delta_ends_a_restart_whose_birth_did_not_change() {
        use crate::snapshot::Full;
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        let restart = op(&id, &inc, "restart-node:mesh1.rpc.2");
        let ready = digest(&id, &inc, None, MemberStatus::ReadyForTraffic, 9);
        let restarting = Full::new(vec![ready.clone()], vec![restart], Vec::new());
        let started = Full::new(vec![ready], Vec::new(), Vec::new());
        let delta = restarting.delta_to(&started);
        let mut receiver = restarting.clone();
        receiver.apply(&delta);
        assert!(receiver.same_topology(&started), "the receiver holds what the source holds: {delta:?}");
    }
}
