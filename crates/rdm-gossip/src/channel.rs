//! One subscribed gossip channel (gossip.md §4): its sender, its known peers, its stop and start,
//! repair and refeed, and the channel ids of a mesh's topology channel and the backbone.


use rdm_transport::clock::SharedClock;
use anyhow::Result;
use bytes::Bytes;
use futures_lite::StreamExt as _;
use iroh::{Endpoint, EndpointAddr};
use iroh_gossip::api::{Event, GossipSender};
use iroh_gossip::net::Gossip;
use iroh_gossip::proto::TopicId;
use rdm_core::{FabricId, MeshDigest, MeshId};
use std::collections::BTreeSet;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rdm_core::cadence::{backbone_gossip_interval, staleness_floor};

use crate::{frame::*, location::*, digest_view::*};

/// A mesh's topology channel.
pub fn mesh_channel(fabric: &FabricId, mesh_id: &MeshId) -> TopicId {
    TopicId::from_bytes(*blake3::hash(format!("rafka-mesh-membership:{fabric}:{mesh_id}").as_bytes()).as_bytes())
}

/// The fabric's backbone: node-admins only.
pub fn backbone_channel(fabric: &FabricId) -> TopicId {
    TopicId::from_bytes(*blake3::hash(format!("rafka-fabric-backbone:{fabric}").as_bytes()).as_bytes())
}

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
/// is no longer held (a deleted birth) is never a target, and its schedule entry is
/// dropped with it. Pure: the caller makes the attempts.
pub struct RepairSchedule {
    pub(crate) window: Duration,
    pub(crate) last: std::collections::HashMap<iroh::EndpointId, Instant>,
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

/// The held members of `digest_view` that `pick` selects, each with how long it has gone unheard; a
/// member held `Leaving` is never a repair target.
pub(crate) fn held_targets(digest_view: &DigestView, pick: impl Fn(&MeshDigest) -> bool) -> Vec<RepairTarget> {
    let now = Instant::now();
    // A member under an open removal is held through it, but its address is a birth's
    // that is going or gone: it is never handed to a channel again.
    let overlaid = overlaid_nodes(digest_view);
    digest_view.silent_held(now)
        .into_iter()
        .filter(|(d, _)| pick(d) && d.status != rdm_core::MemberStatus::Leaving && !overlaid.contains(d.node.node_id.as_str()))
        .filter_map(|(d, silent_for)| gossip_addr(&d).map(|addr| RepairTarget { addr, node: d.node.node_id.to_string(), node_id: d.node.node_id.to_string(), silent_for }))
        .collect()
}

/// The nodes `digest_view` holds under an open lifecycle overlay (a removal in flight).
pub(crate) fn overlaid_nodes(digest_view: &DigestView) -> BTreeSet<String> {
    digest_view.in_flight().into_iter().map(|op| op.node_id.to_string()).collect()
}

/// What a channel reads from its view each repair tick: the held members due a repair attempt,
/// and the keys whose birth is under an open removal, whose addresses the channel
/// forgets.
pub struct Held {
    /// The members due a repair attempt.
    pub targets: Vec<RepairTarget>,
    /// The keys of births under an open removal, whose addresses the channel forgets.
    pub deleted: Vec<iroh::EndpointId>,
}

/// [`Held`] from `digest_view` for the members `pick` selects.
pub(crate) fn held_view(digest_view: &DigestView, pick: impl Fn(&MeshDigest) -> bool + Copy) -> Held {
    let overlaid = overlaid_nodes(digest_view);
    let deleted = digest_view
        .all()
        .into_iter()
        .filter(|d| pick(d) && overlaid.contains(d.node.node_id.as_str()))
        .filter_map(|d| d.node.endpoint_id.0.parse::<iroh::PublicKey>().ok())
        .collect();
    Held { targets: held_targets(digest_view, pick), deleted }
}

/// The members of `live` not yet in `joined`, recording them as joined; members that left `live`
/// are dropped from `joined`.
pub fn rejoin(joined: &Mutex<BTreeSet<iroh::EndpointId>>, live: &[iroh::EndpointId]) -> Vec<iroh::EndpointId> {
    let mut joined = joined.lock().unwrap();
    joined.retain(|id| live.contains(id));
    live.iter().copied().filter(|id| joined.insert(*id)).collect()
}

/// One joined gossip channel. iroh-gossip closes a subscriber that falls behind
/// (`Lagged`) and expects it to be re-opened; a subscription can also end.
/// Either way it is re-subscribed through its seeds and every peer it was
/// asked to join, so it never silently stops hearing the channel.
///
/// The topology view through iroh-gossip's own API (`gossip.md` §6): a peer that
/// leaves the live list it is asked to join and returns is joined again, and
/// a channel with no neighbour for one staleness floor is handed every peer it
/// knows again, once per window while that holds. A node whose neighbours all
/// dropped (a partition longer than the connections' idle timeout) is
/// otherwise never dialed again. Nothing is sent.
#[derive(Clone)]
pub(crate) struct Channel {
    /// The channel's label (`mesh:<name>`, `backbone`) and id, for the dial spans.
    pub(crate) label: String,
    pub(crate) channel_id: TopicId,
    /// The channel's sender; `None` while the channel is stopped (the channel is left).
    pub(crate) sender: Arc<tokio::sync::RwLock<Option<GossipSender>>>,
    /// `true` while the channel is stopped: the channel is left and nothing is sent or heard on it.
    pub(crate) stopped: Arc<tokio::sync::watch::Sender<bool>>,
    /// Counts the times the receive task has finished leaving the channel for a stop.
    pub(crate) left: Arc<tokio::sync::watch::Sender<u64>>,
    /// Every peer this channel was seeded with or asked to join.
    pub(crate) peers: Arc<Mutex<BTreeSet<iroh::EndpointId>>>,
    /// The peers asked while they stayed on the live list.
    pub(crate) joined: Arc<Mutex<BTreeSet<iroh::EndpointId>>>,
    /// The endpoint's peer-location view: shared by every channel of the endpoint.
    pub(crate) location_view: LocationView,
    pub(crate) endpoint: Endpoint,
    /// The active gossip neighbours on this channel (NeighborUp minus NeighborDown).
    pub(crate) neighbors: Arc<Mutex<BTreeSet<iroh::EndpointId>>>,
    /// Counts the changes of `neighbors`: what a waiter on "a neighbour is up" wakes on.
    pub(crate) neighbors_changed: Arc<tokio::sync::watch::Sender<u64>>,
    /// Frames broadcast on this channel since this birth.
    pub(crate) frames_sent: Arc<std::sync::atomic::AtomicU64>,
    /// Frames decoded from this channel since this birth.
    pub(crate) frames_received: Arc<std::sync::atomic::AtomicU64>,
    pub(crate) author: Arc<Mutex<Option<crate::envelope::Birth>>>,
    pub(crate) clock: SharedClock,
}

/// Classify the families whose ordering state ReceiveOrder holds. Lifecycle identities are
/// recorded before their carried revision is judged, even on a revision refusal. Digest is
/// classified once, against its installed content under the digest view's lock; it has no
/// second receive-history floor. Every ordering refusal is named via-stale.
pub(crate) fn admitted(order: &mut crate::receive_order::ReceiveOrder, f: &Frame, node: &str, channel: &str) -> bool {
    let first_fact = order.admit_lifecycle_identity(f);
    match order.admit(f) {
        Ok(()) => first_fact,
        Err(e) => {
            tracing::info_span!(
                "rdm.mesh.topology.reject.via-stale",
                node, channel, frame = e.frame, subject = %e.source, held_key = %e.held, frame_key = %e.offered,
            ).in_scope(|| tracing::info!("the frame's replacement revision is below the held one"));
            false
        }
    }
}

/// What a primary replays on `NeighborUp`: either `direct` frames for the channel to broadcast
/// (on the backbone, which has no forward queue) or frames already queued onto [`ForwardPath`]
/// under the forwarder's lock (on the mesh channel, so no concurrent delta can pass the replay).
#[derive(Default)]
pub(crate) struct Replayed {
    pub(crate) direct: Vec<Frame>,
    pub(crate) frames: usize,
    pub(crate) full_chunks: usize,
    pub(crate) bytes: usize,
}

impl Replayed {
    pub(crate) fn direct(direct: Vec<Frame>, author: &crate::envelope::Birth, now_ms: u64) -> Self {
        let direct: Vec<_> = direct.into_iter().map(|f| {
            assert!(f.context().is_some() || !matches!(f.data(), Frame::MeshStatus { .. } | Frame::FabricStatus { .. }), "a replay retains status provenance; it cannot invent a revision");
            f.authored(author, 0, now_ms)
        }).collect();
        let bytes = direct.iter().map(|f| f.encode().len()).sum();
        let full_chunks = direct.iter().filter(|f| matches!(f.data(), Frame::MeshTopology { .. })).count();
        let frames = direct.len();
        Self { direct, frames, full_chunks, bytes }
    }
}

impl Channel {
    /// Register where the birth `d` names is, and delete the paths a moved key leaves behind.
    pub(crate) fn register(&self, d: &MeshDigest) {
        self.location_view.register(d);
    }

    pub(crate) async fn join(
        gossip: &Gossip,
        endpoint: &Endpoint,
        channel_id: TopicId,
        fabric: &str,
        node: &str,
        channel: &str,
        seeds: Vec<EndpointAddr>,
        location_view: LocationView,
        order: Arc<Mutex<crate::receive_order::ReceiveOrder>>,
        on_frame: Arc<dyn Fn(Frame, &mut crate::receive_order::ReceiveOrder) + Send + Sync>,
        targets: Arc<dyn Fn() -> Held + Send + Sync>,
        replay: Arc<dyn Fn() -> Replayed + Send + Sync>,
        author: Arc<Mutex<Option<crate::envelope::Birth>>>,
        clock: SharedClock,
    ) -> Result<Self> {
        for s in &seeds {
            location_view.fill(s);
        }
        let peers: BTreeSet<iroh::EndpointId> = seeds.iter().map(|s| s.id).collect();
        crate::gossip::dial_span("topology.subscribe", &format!("{}@{}", location_view.observer_name(), endpoint.id().fmt_short()), channel, &channel_id, &peers.iter().copied().collect::<Vec<_>>(), &peers.iter().copied().collect::<Vec<_>>());
        let sub = gossip.subscribe(channel_id, peers.iter().copied().collect()).await?;
        tracing::info_span!("rdm.mesh.topology.update.via-subscribe", node = %location_view.observer_name(), channel, fabric, peers = peers.len())
            .in_scope(|| tracing::info!("subscribed"));
        let (sender, receiver) = sub.split();
        let joined = Arc::new(Mutex::new(peers.clone()));
        let me = Self { label: channel.to_string(), channel_id, sender: Arc::new(tokio::sync::RwLock::new(Some(sender))), stopped: Arc::new(tokio::sync::watch::Sender::new(false)), left: Arc::new(tokio::sync::watch::Sender::new(0)), peers: Arc::new(Mutex::new(peers)), joined, location_view, endpoint: endpoint.clone(), neighbors: Arc::default(), neighbors_changed: Arc::new(tokio::sync::watch::Sender::new(0)), frames_sent: Arc::default(), frames_received: Arc::default(), author, clock };
        let neighbors = me.neighbors.clone();
        let neighbors_changed = me.neighbors_changed.clone();
        let (sent, received) = (me.frames_sent.clone(), me.frames_received.clone());
        let held_members = targets.clone();
        me.refeed(channel.to_string(), neighbors.clone(), targets);
        let (shared, known, gossip, fabric, channel, location_view) = (me.sender.clone(), me.peers.clone(), gossip.clone(), fabric.to_string(), channel.to_string(), me.location_view.clone());
        let (stopped, left) = (me.stopped.clone(), me.left.clone());
        let _ = node;
        tokio::spawn(async move {
            let mut receiver = Some(receiver);
            loop {
                let mut stopped_rx = stopped.subscribe();
                let reason = loop {
                    let ev = tokio::select! {
                        ev = receiver.as_mut().expect("the receiver is held while the channel is open").next() => ev,
                        _ = stopped_rx.wait_for(|p| *p) => break "stopped".to_string(),
                    };
                    let node = location_view.observer_name();
                    match &ev {
                        Some(Ok(Event::Received(m))) => {
                            let received_event = crate::envelope::decode_received(&m.content);
                            let span = tracing::info_span!("rdm.mesh.topology.update.via-frame", node = %node, channel = %channel, sender = %m.delivered_from.fmt_short(), bytes = m.content.len());
                            if let Some(parent) = received_event.trace.traceparent.as_deref() {
                                rdm_transport::set_remote_parent(&span, parent, received_event.trace.tracestate.as_deref());
                            }
                            span.in_scope(|| {
                                if !received_event.dropped.is_empty() {
                                    tracing::info_span!("rdm.mesh.topology.update.via-context-dropped", node = %node, channel = %channel, context_dropped = %rdm_core::context::dropped_as_str(&received_event.dropped))
                                        .in_scope(|| tracing::info!("malformed tracing metadata dropped; the frame continues"));
                                }
                                match received_event.frame {
                                    Ok(f) => {
                                        if f.context().is_none_or(|c| c.source.fabric.as_str() != fabric) {
                                            tracing::info_span!("rdm.mesh.topology.reject.via-undecodable-frame", node = %node, channel = %channel, sender = %m.delivered_from.fmt_short(), bytes = m.content.len(), error = "source fabric differs from the channel")
                                                .in_scope(|| tracing::info!("the source does not belong to this fabric"));
                                            return;
                                        }
                                        received.fetch_add(1, Ordering::Relaxed);
                                        // The one ordering check of the receive path (R-RX3), against the
                                        // node's one held version per source, shared by every channel and
                                        // the topology-read install: a frame received after a newer one of
                                        // its source is ignored, by name, before any handler reads it. The
                                        // check and what the handler installs are one hold of it.
                                        let receipt = match f.data() {
                                            Frame::NodeDeleting { op, topology_version, .. } | Frame::NodeStopped { op, topology_version, .. } => Some(tracing::info_span!(
                                                "rdm.mesh.topology.update.via-lifecycle-receipt",
                                                node = %node, build_id = %op.build_id, operation = %op.operation.action, operation_subject = ?op.operation.subject,
                                                node_id = %op.node_id, incarnation_id = %op.incarnation, "type" = %op.r#type, topology_version,
                                            )),
                                            _ => None,
                                        };
                                        let _receipt = receipt.as_ref().map(tracing::Span::enter);
                                        let mut held = order.lock().unwrap();
                                        if admitted(&mut held, &f, &node, &channel) {
                                            on_frame(f, &mut held);
                                        }
                                    }
                                    // A frame this build does not read is refused by name, with who
                                    // sent it, never dropped as if it had not arrived.
                                    Err(crate::envelope::DecodeError::Reserved(kind, position)) => tracing::info_span!(
                                        "rdm.mesh.topology.reject.via-reserved", node = %node, channel = %channel, frame = reserved_name(position), event_type = %kind, position, via = if channel == "backbone" { "backbone" } else { "mesh-channel" }, ignored = true,
                                    ).in_scope(|| tracing::info!("reserved type refused before data decode")),
                                    Err(crate::envelope::DecodeError::Unknown(kind)) => tracing::info_span!(
                                        "rdm.mesh.topology.reject.via-unknown-type", node = %node, channel = %channel, frame = %kind, ignored = true,
                                    ).in_scope(|| tracing::info!("unknown type refused before data decode")),
                                    Err(crate::envelope::DecodeError::Malformed(e)) => tracing::info_span!(
                                        "rdm.mesh.topology.reject.via-undecodable-frame",
                                        node = %node,
                                        channel = %channel,
                                        sender = %m.delivered_from.fmt_short(),
                                        bytes = m.content.len(),
                                        error = %e
                                    )
                                    .in_scope(|| tracing::info!("a gossip frame does not decode")),
                                }
                            });
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
                            let replayed = if sender.is_some() { replay() } else { Replayed::default() };
                            if replayed.frames > 0 {
                                tracing::info_span!("rdm.mesh.topology.update.via-neighbour-replay", node = %node, channel = %channel, peer = %p.fmt_short(), frames = replayed.frames, full_chunks = replayed.full_chunks, bytes = replayed.bytes)
                                    .in_scope(|| tracing::info!("a neighbour came up: this primary says what it holds once"));
                            }
                            if let Some(sender) = sender {
                                for f in replayed.direct {
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
                if reason == "stopped" {
                    // The channel is left: the sender and the receiver are both dropped, so this node
                    // is no neighbour of anyone on it. It joins again only when the channel is started.
                    *shared.write().await = None;
                    drop(receiver.take());
                    left.send_modify(|n| *n += 1);
                    tracing::info_span!("rdm.mesh.connection.delete.via-channel-stopped", node = %location_view.observer_name(), channel = %channel)
                        .in_scope(|| tracing::info!("the channel is stopped: it is left"));
                    let _ = stopped_rx.wait_for(|p| !*p).await;
                }
                // Back on its channel, a node dials the members of its mesh it holds (gossip.md §6's
                // held members, their addresses known), with the peers it was seeded or repaired
                // through: a node-admin's seed is the admin that launched it, which for a mesh's first
                // admin is the fabric-primary, on the backbone and not on this channel (Q88, Luke
                // 2026-10-20: a node-admin on a mesh with another node-admin is never without a
                // neighbour on its mesh channel).
                for t in held_members().targets {
                    location_view.fill(&t.addr);
                    known.lock().unwrap().insert(t.addr.id);
                }
                let peers: Vec<iroh::EndpointId> = known.lock().unwrap().iter().copied().collect();
                let span = tracing::info_span!("rdm.mesh.topology.update.via-resubscribe", node = %location_view.observer_name(), fabric = %fabric, channel = %channel, reason = %reason, peers = peers.len());
                // A refused subscribe means the gossip actor itself has stopped.
                crate::gossip::dial_span("topology.reopen", &location_view.observer_name(), &channel, &channel_id, &peers, &peers);
                let reopened = match gossip.subscribe(channel_id, peers).await {
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

    pub(crate) fn authored(&self, f: &Frame) -> Result<Frame> {
        if f.context().is_some() { return Ok(f.clone()); }
        anyhow::ensure!(!matches!(f.data(), Frame::MeshStatus { .. } | Frame::FabricStatus { .. }), "a status requires its original author and held committed revision");
        let author = self.author.lock().unwrap().clone().ok_or_else(|| anyhow::anyhow!("author birth has not been supplied"))?;
        Ok(f.clone().authored(&author, 0, self.clock.now_rafka_ms()))
    }

    pub(crate) async fn broadcast(&self, f: &Frame) -> Result<()> {
        let sender = self.sender.read().await.clone().ok_or_else(|| anyhow::anyhow!("the channel is stopped: it is left, nothing is sent"))?;
        let f = self.authored(f)?;
        let bytes = f.encode();
        anyhow::ensure!(bytes.len() <= MAX_FRAME, "complete gossip message {} exceeds MAX_FRAME {}", bytes.len(), MAX_FRAME);
        sender.broadcast(Bytes::from(bytes)).await?;
        self.frames_sent.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Leave the channel: the sender and the receiver are dropped, so this node is a neighbour of no
    /// one on it, and nothing is sent or heard until [`Self::start`]. Resolves once the channel is
    /// left. Stopping a stopped channel is a no-op.
    pub(crate) async fn stop(&self) {
        if *self.stopped.borrow() {
            return;
        }
        let mut left = self.left.subscribe();
        left.borrow_and_update();
        self.stopped.send_replace(true);
        let _ = left.changed().await;
    }

    /// Join the channel again through the peers this channel knows.
    pub(crate) async fn start(&self) {
        let mut ready = self.sender.read().await.is_some();
        self.stopped.send_replace(false);
        while !ready {
            tokio::time::sleep(Duration::from_millis(5)).await;
            ready = self.sender.read().await.is_some();
        }
    }

    /// Register `peer`'s address; `true` when it is new to this channel.
    pub(crate) fn learn(&self, peer: &EndpointAddr) -> bool {
        self.location_view.fill(peer);
        self.peers.lock().unwrap().insert(peer.id)
    }

    /// Join the peers of the live list `peers` not asked while live: a peer
    /// that left the list and returns is asked again.
    pub(crate) async fn join_peers(&self, caller: &'static str, peers: Vec<EndpointAddr>) -> Result<usize> {
        for p in &peers {
            self.learn(p);
        }
        let live: Vec<iroh::EndpointId> = peers.iter().map(|p| p.id).collect();
        let fresh = rejoin(&self.joined, &live);
        let sender = self.sender.read().await.clone();
        // The span names what the caller named and what is handed to iroh-gossip: a peer already asked is
        // named and not dialled again, and a stopped channel dials nothing.
        let dialled: &[iroh::EndpointId] = if sender.is_some() { &fresh } else { &[] };
        crate::gossip::dial_span(caller, &format!("{}@{}", self.location_view.observer_name(), self.endpoint.id().fmt_short()), &self.label, &self.channel_id, &live, dialled);
        let Some(sender) = sender else { return Ok(0) };
        if !fresh.is_empty() {
            sender.join_peers(fresh.clone()).await?;
        }
        Ok(fresh.len())
    }

    /// While the channel has no neighbour, hand it every peer it knows again: a bounded
    /// join attempt one gossip interval after it was left alone, then backed off (doubling, at
    /// most one staleness floor apart) while that holds (PRD §6.2). A neighbour coming up resets
    /// it. A node whose connections all timed out (a wedge, a network gap longer than the idle
    /// timeout) is otherwise heard again only a whole floor later.
    pub(crate) fn refeed(&self, channel: String, neighbors: Arc<Mutex<BTreeSet<iroh::EndpointId>>>, targets: Arc<dyn Fn() -> Held + Send + Sync>) {
        let me = self.clone();
        tokio::spawn(async move {
            let mut alone_since: Option<Instant> = None;
            let mut backoff = backbone_gossip_interval();
            let mut schedule = RepairSchedule::new(staleness_floor());
            let mut deleted: BTreeSet<iroh::EndpointId> = BTreeSet::new();
            loop {
                tokio::time::sleep(backbone_gossip_interval()).await;
                let node = me.location_view.observer_name();
                // Held-member repair (gossip.md §6): every held member whose coverage has gone stale
                // for the repair window gets a bounded join attempt, whatever this channel's
                // neighbours; a failed attempt decides nothing about the member.
                let held = targets();
                // A birth under an open removal: this channel keeps no address for
                // its key and never hands it out again. The birth's iroh paths stay open: its
                // typed connections delete is in flight over them, and they are deleted at the exited verdict.
                for key in &held.deleted {
                    if deleted.insert(*key) {
                        me.location_view.forget(*key);
                        me.peers.lock().unwrap().remove(key);
                        me.joined.lock().unwrap().remove(key);
                        tracing::info_span!("rdm.mesh.connection.update.via-restart-address-removed", node = %node, channel = %channel, peer = %key.fmt_short())
                            .in_scope(|| tracing::info!("a birth under an open removal: its address is dropped from this channel; its paths stay until the exit is proven"));
                    }
                }
                deleted.retain(|k| held.deleted.contains(k));
                for t in schedule.due(held.targets, Instant::now()) {
                    me.location_view.fill(&t.addr);
                    me.peers.lock().unwrap().insert(t.addr.id);
                    let sender = me.sender.read().await.clone();
                    let joined = match sender {
                        Some(sender) => {
                            crate::gossip::dial_span("topology.repair", &format!("{node}@{}", me.endpoint.id().fmt_short()), &channel, &me.channel_id, &[t.addr.id], &[t.addr.id]);
                            sender.join_peers(vec![t.addr.id]).await.is_ok()
                        }
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
                // No neighbour: every held member of the mesh (Q88) and every known peer is handed to the channel again.
                for t in targets().targets {
                    me.location_view.fill(&t.addr);
                    me.peers.lock().unwrap().insert(t.addr.id);
                }
                let peers: Vec<iroh::EndpointId> = me.peers.lock().unwrap().iter().copied().collect();
                if peers.is_empty() {
                    continue;
                }
                let sender = me.sender.read().await.clone();
                let joined = match sender {
                    Some(sender) => {
                        crate::gossip::dial_span("topology.refeed", &format!("{node}@{}", me.endpoint.id().fmt_short()), &channel, &me.channel_id, &peers, &peers);
                        sender.join_peers(peers.clone()).await.is_ok()
                    }
                    None => continue,
                };
                tracing::info_span!("rdm.mesh.connection.update.via-refeed", node = %node, channel = %channel, reason = "no-neighbours-fallback", peers = peers.len(), joined)
                    .in_scope(|| tracing::info!("no neighbour: every known peer handed to the channel again"));
                alone_since = Some(Instant::now());
            }
        });
    }
}

/// Why a subscription was re-opened, if it was.
pub(crate) fn ended(ev: Option<Result<Event, iroh_gossip::api::ApiError>>) -> Option<String> {
    match ev {
        None => Some("subscription ended".into()),
        Some(Err(e)) => Some(format!("subscription error: {e}")),
        Some(Ok(Event::Lagged)) => Some("lagged: events were dropped".into()),
        Some(Ok(_)) => None,
    }
}

/// Which channel a snapshot frame arrived on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Side {
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
    /// out of the view (the reader maps it from the read itself).
    ReadPeer,
}

impl Side {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Side::Backbone => "backbone",
            Side::MeshChannel => "mesh-channel",
            Side::Read | Side::ReadPeer => "topology-read",
        }
    }

    pub(crate) fn via(self) -> &'static str {
        match self {
            Side::Backbone => "backbone",
            Side::MeshChannel => "forwarded",
            Side::Read | Side::ReadPeer => "read",
        }
    }
}
