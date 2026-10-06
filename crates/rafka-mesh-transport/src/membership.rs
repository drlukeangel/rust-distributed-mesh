//! Hierarchical fabric membership over iroh-gossip (HyParView + Plumtree;
//! never a hand-rolled delivery layer). `docs/i143/design.md` §2.2 and
//! rafka `gossip.md` §6:
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
//! (its first endpoint slot), so HyParView reaches a peer it learned by id.

use anyhow::Result;
use bytes::Bytes;
use futures_lite::StreamExt as _;
use iroh::address_lookup::MemoryLookup;
use iroh::{Endpoint, EndpointAddr};
use iroh_gossip::api::{Event, GossipSender};
use iroh_gossip::net::Gossip;
use iroh_gossip::proto::TopicId;
use rafka_mesh_entity::{FabricId, MeshDigest, MeshId};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// How long a member stays heard without a fresh word.
pub const SILENT_AFTER: Duration = Duration::from_secs(3);
/// The backbone publication cadence.
pub const PUBLISH_EVERY: Duration = Duration::from_millis(1000);
/// The largest encoded frame: iroh-gossip's default maximum message is 4096
/// bytes; the rest is its own framing.
const MAX_FRAME: usize = 3800;

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

/// A member's gossip address: its key at its first endpoint slot.
pub fn gossip_addr(d: &MeshDigest) -> Option<EndpointAddr> {
    let key = d.node.transport_id.0.parse::<iroh::PublicKey>().ok()?;
    let slot = d.node.endpoints.0.first()?;
    Some(EndpointAddr::new(key).with_ip_addr(slot.addr))
}

/// `RAFKA_LEAVE_LINGER_MS` (default 1000): how long a stopping node keeps
/// announcing `Leaving` before it closes. Every node kind takes the same one.
pub fn leave_linger_from_env() -> Duration {
    Duration::from_millis(std::env::var("RAFKA_LEAVE_LINGER_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(1000))
}

/// How often a leaving node repeats its `Leaving` during the linger.
pub const LEAVE_EVERY: Duration = Duration::from_millis(200);

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

/// What travels on a mesh channel or the backbone.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "frame", rename_all = "kebab-case")]
pub enum Frame {
    /// A member's own digest (its mesh channel only).
    Digest { digest: MeshDigest },
    /// Members of `mesh`, published on the backbone by that mesh's primary
    /// (`publisher`), packed into as few frames as fit one gossip message, and
    /// forwarded onto a peer mesh's channel by its primary (`forwarded_by`).
    /// `sent_unix_ms` makes each publication distinct.
    Members { mesh: String, publisher: String, forwarded_by: Option<String>, sent_unix_ms: u64, digests: Vec<MeshDigest> },
    /// The fabric's status, published by the fabric primary alone.
    FabricStatus { fabric: FabricId, status: String, publisher: String, forwarded_by: Option<String>, sent_unix_ms: u64 },
}

impl Frame {
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("frame serializes")
    }
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        serde_json::from_slice(bytes).ok()
    }
}

/// The fabric status a node last heard, with its publisher.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FabricStatus {
    pub status: String,
    pub publisher: String,
}

fn now_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

/// The peers of the live list `live` to join now: those not joined while
/// they stayed live. `joined` forgets a peer that left the list, so one that
/// returns is joined again (`gossip.md` §6).
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
/// a channel with no neighbour for [`SILENT_AFTER`] is handed every peer it
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
}

impl Channel {
    async fn join(
        gossip: &Gossip,
        endpoint: &Endpoint,
        topic: TopicId,
        fabric: &str,
        node: &str,
        channel: &str,
        seeds: Vec<EndpointAddr>,
        on_frame: Arc<dyn Fn(Frame) + Send + Sync>,
    ) -> Result<Self> {
        let lookup = MemoryLookup::new();
        endpoint.address_lookup().map_err(|e| anyhow::anyhow!("address_lookup: {e}"))?.add(lookup.clone());
        for s in &seeds {
            lookup.add_endpoint_info(s.clone());
        }
        let peers: BTreeSet<iroh::EndpointId> = seeds.iter().map(|s| s.id).collect();
        let sub = gossip.subscribe(topic, peers.iter().copied().collect()).await?;
        tracing::info_span!("rafka.mesh.membership.update.via-subscribe", node, channel, fabric, peers = peers.len())
            .in_scope(|| tracing::info!("subscribed"));
        let (sender, mut receiver) = sub.split();
        let joined = Arc::new(Mutex::new(peers.clone()));
        let me = Self { sender: Arc::new(tokio::sync::RwLock::new(sender)), peers: Arc::new(Mutex::new(peers)), joined, lookup };
        let neighbors: Arc<Mutex<BTreeSet<iroh::EndpointId>>> = Arc::default();
        me.refeed(node.to_string(), channel.to_string(), neighbors.clone());
        let (shared, known, gossip, fabric, channel) = (me.sender.clone(), me.peers.clone(), gossip.clone(), fabric.to_string(), channel.to_string());
        tokio::spawn(async move {
            loop {
                let reason = loop {
                    let ev = receiver.next().await;
                    match &ev {
                        Some(Ok(Event::Received(m))) => {
                            if let Some(f) = Frame::decode(&m.content) {
                                on_frame(f);
                            }
                            continue;
                        }
                        Some(Ok(Event::NeighborUp(p))) => {
                            neighbors.lock().unwrap().insert(*p);
                        }
                        Some(Ok(Event::NeighborDown(p))) => {
                            neighbors.lock().unwrap().remove(p);
                        }
                        _ => {}
                    }
                    if let Some(r) = ended(ev) {
                        break r;
                    }
                };
                neighbors.lock().unwrap().clear();
                let peers: Vec<iroh::EndpointId> = known.lock().unwrap().iter().copied().collect();
                let span = tracing::info_span!("rafka.mesh.membership.update.via-resubscribe", fabric = %fabric, channel = %channel, reason = %reason, peers = peers.len());
                // A refused subscribe means the gossip actor itself has stopped.
                let reopened = match gossip.subscribe(topic, peers).await {
                    Ok(t) => t,
                    Err(e) => {
                        span.in_scope(|| tracing::info!(error = %e, "gossip has stopped; the channel ends"));
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

    /// While the channel has had no neighbour for a whole window, hand it
    /// every peer it knows again, once per window.
    fn refeed(&self, node: String, channel: String, neighbors: Arc<Mutex<BTreeSet<iroh::EndpointId>>>) {
        let me = self.clone();
        tokio::spawn(async move {
            let mut alone_since: Option<Instant> = None;
            loop {
                tokio::time::sleep(PUBLISH_EVERY).await;
                if !neighbors.lock().unwrap().is_empty() {
                    alone_since = None;
                    continue;
                }
                let since = *alone_since.get_or_insert_with(Instant::now);
                if since.elapsed() < SILENT_AFTER {
                    continue;
                }
                let peers: Vec<iroh::EndpointId> = me.peers.lock().unwrap().iter().copied().collect();
                if peers.is_empty() {
                    continue;
                }
                let sender = me.sender.read().await.clone();
                let joined = sender.join_peers(peers.clone()).await.is_ok();
                tracing::info_span!("rafka.mesh.connection.update.via-refeed", node = %node, channel = %channel, peers = peers.len(), joined)
                    .in_scope(|| tracing::info!("no neighbour for a window: every known peer handed to the channel again"));
                alone_since = Some(Instant::now());
            }
        });
    }
}

/// Split `digests` into runs whose frame (as `frame` builds it, sized as a
/// forward would be) encodes within [`MAX_FRAME`]. A digest alone too large
/// travels alone.
fn pack(digests: Vec<MeshDigest>, frame: impl Fn(Vec<MeshDigest>) -> Frame) -> Vec<Vec<MeshDigest>> {
    let mut out: Vec<Vec<MeshDigest>> = Vec::new();
    let mut run: Vec<MeshDigest> = Vec::new();
    for d in digests {
        run.push(d);
        if run.len() > 1 && frame(run.clone()).encode().len() > MAX_FRAME {
            let last = run.pop().expect("just pushed");
            out.push(std::mem::take(&mut run));
            run.push(last);
        }
    }
    if !run.is_empty() {
        out.push(run);
    }
    out
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

/// What a node's view holds, fed by its mesh channel (and, for an admin, the
/// backbone): every member's latest digest, the fabric status, and where it
/// learned each mesh from.
#[derive(Clone, Default)]
struct View {
    book: DigestBook,
    fabric_status: Arc<Mutex<Option<(FabricStatus, Instant)>>>,
    via: Arc<Mutex<HashMap<String, &'static str>>>,
}

impl View {
    /// Hold what `f` carries; the digests it carries (for their addresses).
    fn take(&self, f: &Frame, fabric: &FabricId, via: &'static str) -> Vec<MeshDigest> {
        match f {
            Frame::Digest { digest } if &digest.fabric_id == fabric => {
                if self.book.record(digest.clone()) {
                    self.note(digest, via);
                }
                vec![digest.clone()]
            }
            Frame::Members { digests, .. } => {
                let mine: Vec<MeshDigest> = digests.iter().filter(|d| &d.fabric_id == fabric).cloned().collect();
                for d in &mine {
                    if self.book.record_forwarded(d.clone()) {
                        self.note(d, via);
                    }
                }
                mine
            }
            Frame::FabricStatus { fabric: f, status, publisher, .. } if f == fabric => {
                *self.fabric_status.lock().unwrap() = Some((FabricStatus { status: status.clone(), publisher: publisher.clone() }, Instant::now()));
                Vec::new()
            }
            _ => Vec::new(),
        }
    }

    fn note(&self, d: &MeshDigest, via: &'static str) {
        self.via.lock().unwrap().entry(d.node.name.mesh.clone()).or_insert(via);
    }
}

/// A node's membership: its mesh channel and what it holds.
#[derive(Clone)]
pub struct Membership {
    mesh: Channel,
    view: View,
    fabric: FabricId,
    cut_off: Arc<Mutex<CutOff>>,
    pub book: DigestBook,
}

impl Membership {
    /// Join `mesh`'s channel (`mesh_id` names it) through `seeds`, as `node`.
    pub async fn join(gossip: &Gossip, endpoint: &Endpoint, fabric: &FabricId, mesh: &str, mesh_id: &MeshId, node: &str, seeds: Vec<EndpointAddr>) -> Result<Self> {
        let view = View::default();
        let lookup_slot: Arc<Mutex<Option<MemoryLookup>>> = Arc::default();
        let (v, f, ls) = (view.clone(), fabric.clone(), lookup_slot.clone());
        let on_frame: Arc<dyn Fn(Frame) + Send + Sync> = Arc::new(move |frame: Frame| {
            let via = if matches!(frame, Frame::Members { .. }) { "forwarded" } else { "mesh-channel" };
            let carried = v.take(&frame, &f, via);
            if let Some(l) = ls.lock().unwrap().as_ref() {
                for a in carried.iter().filter_map(gossip_addr) {
                    l.add_endpoint_info(a);
                }
            }
        });
        let channel = Channel::join(gossip, endpoint, mesh_topic(fabric, mesh_id), fabric.as_str(), node, &format!("mesh:{mesh}"), seeds, on_frame).await?;
        *lookup_slot.lock().unwrap() = Some(channel.lookup.clone());
        let me = Self { mesh: channel, book: view.book.clone(), view, fabric: fabric.clone(), cut_off: Arc::default() };
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
            loop {
                let current = book.current(SILENT_AFTER);
                let others = current.iter().filter(|d| d.node.name.to_string() != node).count();
                heard_others |= others > 0;
                let off = heard_others && others == 0;
                if let Some(role) = cut_off.lock().unwrap().observe(off, Instant::now()) {
                    tracing::info_span!("rafka.mesh.membership.update.via-cut-off", node = %node, role)
                        .in_scope(|| tracing::info!("no other member is heard: what this node holds is not acted on"));
                }
                let now: BTreeSet<String> = current.into_iter().map(|d| d.node.name.mesh).collect();
                for m in now.difference(&held) {
                    let v = via.lock().unwrap().get(m).copied().unwrap_or("mesh-channel");
                    tracing::info_span!("rafka.mesh.membership.update.via-mesh-learned", node = %node, mesh = %m, via = v)
                        .in_scope(|| tracing::info!("this node holds the mesh"));
                }
                for m in held.difference(&now) {
                    tracing::info_span!("rafka.mesh.membership.update.via-mesh-silent", node = %node, mesh = %m)
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
        if let Some(a) = gossip_addr(&d) {
            self.mesh.lookup.add_endpoint_info(a);
        }
        if self.book.record_forwarded(d.clone()) {
            self.view.note(&d, via);
        }
    }

    /// Cut off: this node heard other members and now hears none. What it
    /// holds then authorizes nothing (an admin executes no Build, publishes
    /// nothing as a primary).
    pub fn is_cut_off(&self) -> bool {
        self.cut_off.lock().unwrap().is_off()
    }

    /// May this node's view authorize anything now: not cut off, and not
    /// within one silence window of healing ([`CutOff::authorizes`]).
    pub fn authorizes(&self) -> bool {
        self.cut_off.lock().unwrap().authorizes(Instant::now())
    }

    /// How many meshes this node holds a live member of.
    pub fn meshes_held(&self) -> usize {
        self.book.current(SILENT_AFTER).into_iter().map(|d| d.node.name.mesh).collect::<BTreeSet<_>>().len()
    }

    /// The fabric status this node last heard, while fresh.
    pub fn fabric_status(&self) -> Option<FabricStatus> {
        self.view.fabric_status.lock().unwrap().as_ref().filter(|(_, at)| at.elapsed() <= SILENT_AFTER * 2).map(|(s, _)| s.clone())
    }

    /// Broadcast this member's digest on its mesh channel (and record it).
    pub async fn publish(&self, d: &MeshDigest) -> Result<()> {
        self.book.record(d.clone());
        self.mesh.broadcast(&Frame::Digest { digest: d.clone() }).await
    }

    /// Broadcast a frame onto this mesh's channel (a forward).
    pub async fn forward(&self, f: &Frame) -> Result<()> {
        self.mesh.broadcast(f).await
    }

    /// Re-broadcast `digest()` every `every` until the returned handle is aborted.
    pub fn publish_every<F>(&self, every: Duration, digest: F) -> tokio::task::JoinHandle<()>
    where
        F: Fn() -> MeshDigest + Send + 'static,
    {
        let me = self.clone();
        tokio::spawn(async move {
            loop {
                let _ = me.publish(&digest()).await;
                tokio::time::sleep(every).await;
            }
        })
    }

    /// Join this mesh channel through more peers (its members, once known).
    pub async fn join_peers(&self, peers: Vec<EndpointAddr>) -> Result<usize> {
        self.mesh.join_peers(peers).await
    }
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
}

impl Backbone {
    pub async fn join(gossip: &Gossip, endpoint: &Endpoint, membership: &Membership, mesh: &str, node: &str, seeds: Vec<EndpointAddr>) -> Result<Self> {
        let forwarding = Arc::new(AtomicBool::new(false));
        let (m, fw, me, own) = (membership.clone(), forwarding.clone(), node.to_string(), mesh.to_string());
        let on_frame: Arc<dyn Fn(Frame) + Send + Sync> = Arc::new(move |frame: Frame| {
            for a in m.view.take(&frame, &m.fabric, "backbone").iter().filter_map(gossip_addr) {
                m.mesh.lookup.add_endpoint_info(a);
            }
            if !fw.load(Ordering::Relaxed) {
                return;
            }
            let forward = match frame {
                Frame::Members { mesh, publisher, sent_unix_ms, digests, .. } if mesh != own => {
                    Some(Frame::Members { mesh, publisher, forwarded_by: Some(me.clone()), sent_unix_ms, digests })
                }
                Frame::FabricStatus { fabric, status, publisher, sent_unix_ms, .. } if publisher != me => {
                    Some(Frame::FabricStatus { fabric, status, publisher, forwarded_by: Some(me.clone()), sent_unix_ms })
                }
                _ => None,
            };
            if let Some(f) = forward {
                let m = m.clone();
                tokio::spawn(async move {
                    let _ = m.forward(&f).await;
                });
            }
        });
        let channel = Channel::join(gossip, endpoint, backbone_topic(&membership.fabric), membership.fabric.as_str(), node, "backbone", seeds, on_frame).await?;
        Ok(Self {
            channel,
            node: node.to_string(),
            mesh: mesh.to_string(),
            fabric: membership.fabric.clone(),
            forwarding,
            publishing: Arc::default(),
            status_publishing: Arc::default(),
        })
    }

    fn role(flag: &AtomicBool, on: bool) -> Option<&'static str> {
        (flag.swap(on, Ordering::Relaxed) != on).then_some(if on { "start" } else { "stop" })
    }

    /// Be (or stop being) this mesh's publisher and forwarder: its primary.
    pub fn set_mesh_primary(&self, primary: bool) {
        if let Some(role) = Self::role(&self.publishing, primary) {
            tracing::info_span!("rafka.mesh.backbone.update.via-aggregate-publisher", node = %self.node, mesh = %self.mesh, role)
                .in_scope(|| tracing::info!("mesh aggregate publication"));
        }
        if let Some(role) = Self::role(&self.forwarding, primary) {
            tracing::info_span!("rafka.mesh.backbone.update.via-forwarder", node = %self.node, mesh = %self.mesh, role)
                .in_scope(|| tracing::info!("peer meshes forwarded onto this mesh's channel"));
        }
    }

    /// Be (or stop being) the fabric-status publisher: the fabric primary.
    pub fn set_fabric_primary(&self, primary: bool) {
        if let Some(role) = Self::role(&self.status_publishing, primary) {
            tracing::info_span!("rafka.mesh.fabric.update.via-status-publisher", node = %self.node, fabric = %self.fabric, role)
                .in_scope(|| tracing::info!("fabric status publication"));
        }
    }

    /// One publication round: this mesh's `members` (while its primary) and
    /// the fabric's `status` (while the fabric primary), on the backbone; the
    /// fabric primary also puts its status on its own mesh channel.
    pub async fn publish(&self, membership: &Membership, members: Vec<MeshDigest>, status: &str) {
        let sent = now_ms();
        if self.publishing.load(Ordering::Relaxed) {
            for digests in pack(members, |digests| Frame::Members {
                mesh: self.mesh.clone(),
                publisher: self.node.clone(),
                // Sized as a peer primary's forward (a node name like this one).
                forwarded_by: Some(self.node.clone()),
                sent_unix_ms: sent,
                digests,
            }) {
                let f = Frame::Members { mesh: self.mesh.clone(), publisher: self.node.clone(), forwarded_by: None, sent_unix_ms: sent, digests };
                let _ = self.channel.broadcast(&f).await;
            }
        }
        if self.status_publishing.load(Ordering::Relaxed) {
            let f = Frame::FabricStatus { fabric: self.fabric.clone(), status: status.into(), publisher: self.node.clone(), forwarded_by: None, sent_unix_ms: sent };
            let _ = self.channel.broadcast(&f).await;
            let _ = membership.forward(&f).await;
            let _ = membership.view.take(&f, &self.fabric, "mesh-channel");
        }
    }

    /// Join the backbone through every node-admin known, once each.
    pub async fn join_admins(&self, admins: Vec<EndpointAddr>) {
        if let Ok(n) = self.channel.join_peers(admins).await {
            if n > 0 {
                tracing::info_span!("rafka.mesh.connection.update.via-backbone-peers-joined", node = %self.node, peers = n)
                    .in_scope(|| tracing::info!("backbone peers joined"));
            }
        }
    }
}

/// How much longer a member heard only through its mesh primary's forwarded
/// aggregate stays heard: one primary succession. The successor hears the
/// loss after `SILENT_AFTER` and publishes on its next round.
pub const SUCCESSION: Duration = Duration::from_millis(SILENT_AFTER.as_millis() as u64 + PUBLISH_EVERY.as_millis() as u64);

/// Whether a node's view may authorize anything. Cut off: it heard other
/// members and now hears none. Healed: it hears one again, but the rest of
/// its view still holds every member it lost as silent until each publishes
/// again, which every live member does within [`SILENT_AFTER`]. A view
/// authorizes nothing while cut off, nor for `SILENT_AFTER` after it healed.
#[derive(Debug, Default)]
pub struct CutOff {
    off: bool,
    healed_at: Option<Instant>,
}

impl CutOff {
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

    pub fn is_off(&self) -> bool {
        self.off
    }

    /// May the view authorize anything at `now`?
    pub fn authorizes(&self, now: Instant) -> bool {
        !self.off && self.healed_at.is_none_or(|t| now.saturating_duration_since(t) >= SILENT_AFTER)
    }
}

/// The latest digest heard per logical node: when it was taken, and whether
/// it came forwarded by its mesh's primary.
#[derive(Debug, Clone)]
pub struct DigestBook {
    inner: Arc<Mutex<HashMap<String, (MeshDigest, Instant, bool)>>>,
    /// Ticks when a member's birth (`MeshDigest::node`) is first held or
    /// changes; a fresher digest of the same birth does not tick.
    births: Arc<tokio::sync::watch::Sender<u64>>,
}

impl Default for DigestBook {
    fn default() -> Self {
        Self { inner: Arc::default(), births: Arc::new(tokio::sync::watch::Sender::new(0)) }
    }
}

/// How long `d` has been silent: since it was taken, or, for a forwarded
/// copy, since one primary succession after that.
fn silence(at: &Instant, forwarded: bool) -> Duration {
    if forwarded {
        at.elapsed().saturating_sub(SUCCESSION)
    } else {
        at.elapsed()
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
            "rafka.mesh.runtime.reject.via-locator-changed",
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
    /// Hold `d` as its member's latest word, unless it is older than what is
    /// held: a digest of the same birth emitted no later than the held one,
    /// or a digest of the birth the held one supersedes. Gossip can deliver
    /// a digest late; a late one never refreshes a silent member or reverts
    /// its status. `false` when `d` was not taken.
    pub fn record(&self, d: MeshDigest) -> bool {
        let mut inner = self.inner.lock().unwrap();
        if let Some((held, _, _)) = inner.get(d.node.node_id.as_str()) {
            if runtime_changed(held, &d) {
                return false;
            }
            let older = if held.node.incarnation == d.node.incarnation {
                d.emitted_unix_ms <= held.emitted_unix_ms
            } else {
                held.node.supersedes.as_ref() == Some(&d.node.incarnation)
            };
            if older {
                return false;
            }
        }
        let birth_changed = inner.get(d.node.node_id.as_str()).is_none_or(|(held, _, _)| held.node != d.node);
        inner.insert(d.node.node_id.to_string(), (d, Instant::now(), false));
        drop(inner);
        if birth_changed {
            self.births.send_modify(|v| *v += 1);
        }
        true
    }

    /// Digests heard within `fresh` of now.
    /// Hold `d` as forwarded by its mesh's primary, which only forwards a
    /// member it hears: an equal copy of the held digest keeps the member
    /// heard (the primary's word that it still is). Otherwise as [`Self::record`].
    pub fn record_forwarded(&self, d: MeshDigest) -> bool {
        let mut inner = self.inner.lock().unwrap();
        if let Some((held, _, _)) = inner.get(d.node.node_id.as_str()) {
            if runtime_changed(held, &d) {
                return false;
            }
            let older = if held.node.incarnation == d.node.incarnation {
                d.emitted_unix_ms < held.emitted_unix_ms
            } else {
                held.node.supersedes.as_ref() == Some(&d.node.incarnation)
            };
            if older {
                return false;
            }
        }
        let birth_changed = inner.get(d.node.node_id.as_str()).is_none_or(|(held, _, _)| held.node != d.node);
        inner.insert(d.node.node_id.to_string(), (d, Instant::now(), true));
        drop(inner);
        if birth_changed {
            self.births.send_modify(|v| *v += 1);
        }
        true
    }

    /// Ticks each time a member's birth is first held or changes (a new
    /// incarnation, a moved slot, a runtime published): what a process's
    /// live resolver is fed from. A fresher digest of the same birth does not.
    pub fn birth_changes(&self) -> tokio::sync::watch::Receiver<u64> {
        self.births.subscribe()
    }

    pub fn current(&self, fresh: Duration) -> Vec<MeshDigest> {
        self.inner.lock().unwrap().values().filter(|(_, at, fw)| silence(at, *fw) <= fresh).map(|(d, _, _)| d.clone()).collect()
    }

    /// The member's latest digest and how long it has been silent.
    pub fn get(&self, node_id: &str) -> Option<(MeshDigest, Duration)> {
        self.inner.lock().unwrap().get(node_id).map(|(d, at, fw)| (d.clone(), silence(at, *fw)))
    }

    pub fn all(&self) -> Vec<MeshDigest> {
        self.inner.lock().unwrap().values().map(|(d, _, _)| d.clone()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rafka_mesh_entity::{EndpointSet, TransportId, IncarnationId, MemberStatus, MeshNode, NodeId};

    fn digest(node_id: &NodeId, incarnation: &IncarnationId, supersedes: Option<IncarnationId>, status: MemberStatus, at: u64) -> MeshDigest {
        MeshDigest {
            fabric_id: FabricId::parse("fab000000001").unwrap(),
            node: MeshNode {
                node_id: node_id.clone(),
                name: "mesh1.rpc.1".parse().unwrap(),
                transport_id: TransportId("key".into()),
                incarnation: incarnation.clone(),
                supersedes,
                endpoints: EndpointSet(vec![]),
                runtime: None,
            },
            status,
            admin_api_base: None,
            emitted_unix_ms: at,
            extra: Default::default(),
            data_dir: None,
        }
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
        assert!(book.record(digest(&id, &birth, None, MemberStatus::Leaving, 200)));
        std::thread::sleep(Duration::from_millis(30));
        assert!(!book.record(digest(&id, &birth, None, MemberStatus::ReadyForTraffic, 100)), "an older digest of the same birth");
        assert!(!book.record(digest(&id, &birth, None, MemberStatus::ReadyForTraffic, 200)), "a duplicate");
        let (held, age) = book.get(id.as_str()).unwrap();
        assert_eq!(held.status, MemberStatus::Leaving, "the status is not reverted");
        assert!(age >= Duration::from_millis(30), "the member's silence is not reset");
        assert!(book.record(digest(&id, &birth, None, MemberStatus::Leaving, 300)), "a newer digest is taken");
    }

    #[test]
    fn members_are_packed_into_frames_that_fit_one_gossip_message() {
        let ds: Vec<MeshDigest> = (0..40).map(|i| digest(&NodeId::mint(), &IncarnationId::mint(), None, MemberStatus::ReadyForTraffic, i)).collect();
        let frame = |digests| Frame::Members { mesh: "mesh1".into(), publisher: "mesh1.admin.1".into(), forwarded_by: Some("mesh2.admin.1".into()), sent_unix_ms: 1, digests };
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
        let frame = |digests| Frame::Members { mesh: "mesh1".into(), publisher: "mesh1.admin.1".into(), forwarded_by: Some("mesh2.admin.1".into()), sent_unix_ms: 1, digests };
        let runs = pack(ds, frame);
        assert_eq!(runs.iter().map(Vec::len).sum::<usize>(), 40, "every digest travels with its runtime");
        for r in &runs {
            assert!(frame(r.clone()).encode().len() <= MAX_FRAME, "a run fits one message");
        }
    }

    #[test]
    fn a_forwarded_copy_keeps_a_member_heard_and_never_reverts_it() {
        let book = DigestBook::default();
        let (id, birth) = (NodeId::mint(), IncarnationId::mint());
        assert!(book.record(digest(&id, &birth, None, MemberStatus::ReadyForTraffic, 200)));
        std::thread::sleep(Duration::from_millis(30));
        assert!(book.record_forwarded(digest(&id, &birth, None, MemberStatus::ReadyForTraffic, 200)), "an equal forwarded copy");
        assert!(book.get(id.as_str()).unwrap().1 < Duration::from_millis(30), "keeps the member heard");
        assert!(!book.record_forwarded(digest(&id, &birth, None, MemberStatus::Pending, 100)), "an older copy is not taken");
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

    /// A peer mesh's members are heard through its primary's forwarded
    /// aggregate. When that primary is lost its successor takes over only
    /// after it hears the loss (`SILENT_AFTER`) and publishes on its next
    /// round: a forwarded member stays heard through one succession, while a
    /// member heard directly falls silent at `SILENT_AFTER`.
    #[test]
    fn a_forwarded_member_stays_heard_through_one_primary_succession() {
        let book = DigestBook::default();
        let (direct, forwarded) = (NodeId::mint(), NodeId::mint());
        assert!(book.record(digest(&direct, &IncarnationId::mint(), None, MemberStatus::ReadyForTraffic, 100)));
        assert!(book.record_forwarded(digest(&forwarded, &IncarnationId::mint(), None, MemberStatus::ReadyForTraffic, 100)));
        std::thread::sleep(SILENT_AFTER + Duration::from_millis(500));
        let heard: Vec<String> = book.current(SILENT_AFTER).into_iter().map(|d| d.node.node_id.to_string()).collect();
        assert!(!heard.contains(&direct.to_string()), "a member heard directly is silent after SILENT_AFTER");
        assert!(heard.contains(&forwarded.to_string()), "a forwarded member is still heard while its mesh's primary is succeeded");
        assert!(book.get(forwarded.as_str()).unwrap().1 <= SILENT_AFTER, "its silence has not begun");
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
        let mut c = CutOff::default();
        assert!(c.authorizes(t0), "never cut off");
        assert_eq!(c.observe(true, t0), Some("start"));
        assert!(!c.authorizes(t0), "cut off");
        let healed = t0 + Duration::from_secs(10);
        assert_eq!(c.observe(false, healed), Some("stop"));
        assert!(!c.authorizes(healed + Duration::from_millis(250)), "healed, but the view still holds lost members as silent");
        assert!(!c.authorizes(healed + SILENT_AFTER - Duration::from_millis(1)));
        assert!(c.authorizes(healed + SILENT_AFTER), "every live member has published again");
        assert_eq!(c.observe(false, healed + SILENT_AFTER), None);
    }
}
