//! A node's gossip channels (gossip.md §4, §5, §8; Q376): [`ChannelId`], the id of a mesh's
//! topology channel, the backbone or an application channel; [`Channels`], the node's one iroh-gossip
//! handle and the channels it holds, with the one join every channel uses ([`Channels::join`]: the
//! first call with an id subscribes, a later one names more peers); and the subscribed channel itself
//! (its sender, its known peers, its stop and start, repair and refeed). The topology view
//! ([`TopologyView`]) is the cache these channels fill; it joins nothing.

use rdm_transport::clock::SharedClock;
use anyhow::Result;
use bytes::Bytes;
use futures_lite::StreamExt as _;
use iroh::{Endpoint, EndpointAddr};
use iroh_gossip::api::{Event, GossipSender};
use iroh_gossip::net::Gossip;
use iroh_gossip::proto::TopicId;
use std::collections::BTreeSet;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use rdm_core::cadence::{backbone_gossip_interval, staleness_floor};
use crate::{frame::*, location::*, digest_view::*, backbone::*, topology::*};
use crate::forward_path::{ForwardPath, ForwardRx};
use crate::snapshot::{Chunk, Gap, PublisherId, Taken};
use rdm_core::{FabricId, IncarnationId, MeshDigest, MeshId};
use std::sync::{Arc, Mutex, OnceLock};

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

/// The one join span (gossip-by-kind.md §5.1): RDM hands `peers` to iroh-gossip on `channel`, a dial
/// when no connection to a peer exists yet. The caller is the span the join runs in; the spawned
/// reopen runs its dial in its `rdm.mesh.topology.update.via-resubscribe` span, and the repair and
/// the refeed in their `rdm.mesh.connection.update.via-refeed` span. `named` is the list the caller named and `peers` the ones handed to
/// iroh-gossip: a peer already asked is named and not dialled again, and the span says so (`named`
/// beside `peers`), never only the dialled ones.
pub(crate) fn dial_span(node: &str, channel: &str, channel_id: &TopicId, named: &[iroh::EndpointId], peers: &[iroh::EndpointId]) {
    if named.is_empty() && peers.is_empty() {
        return;
    }
    let channel_id: String = channel_id.as_bytes()[..4].iter().map(|b| format!("{b:02x}")).collect();
    let short = |list: &[iroh::EndpointId]| list.iter().map(|p| p.fmt_short().to_string()).collect::<Vec<_>>().join(",");
    let (named, peers) = (short(named), short(peers));
    tracing::info_span!("rdm.mesh.connection.update.via-join-peers", node, channel, channel_id = %channel_id, named = %named, peers = %peers)
        .in_scope(|| tracing::info!("peers handed to iroh-gossip"));
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
        dial_span(&format!("{}@{}", location_view.observer_name(), endpoint.id().fmt_short()), channel, &channel_id, &peers.iter().copied().collect::<Vec<_>>(), &peers.iter().copied().collect::<Vec<_>>());
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
                span.in_scope(|| dial_span(&location_view.observer_name(), &channel, &channel_id, &peers, &peers));
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
    pub(crate) async fn join_peers(&self, peers: Vec<EndpointAddr>) -> Result<usize> {
        for p in &peers {
            self.learn(p);
        }
        let live: Vec<iroh::EndpointId> = peers.iter().map(|p| p.id).collect();
        let fresh = rejoin(&self.joined, &live);
        let sender = self.sender.read().await.clone();
        // The span names what the caller named and what is handed to iroh-gossip: a peer already asked is
        // named and not dialled again, and a stopped channel dials nothing.
        let dialled: &[iroh::EndpointId] = if sender.is_some() { &fresh } else { &[] };
        dial_span(&format!("{}@{}", self.location_view.observer_name(), self.endpoint.id().fmt_short()), &self.label, &self.channel_id, &live, dialled);
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
                    // The repair's dial runs in its refeed span: the join span's caller.
                    let span = tracing::info_span!(
                        "rdm.mesh.connection.update.via-refeed",
                        node = %node,
                        channel = %channel,
                        reason = "stale-held-member",
                        peer = %t.addr.id,
                        peer_node = %t.node,
                        peer_node_id = %t.node_id,
                        coverage_age_ms = t.silent_for.as_millis() as u64,
                        peers = 1u64,
                        joined = tracing::field::Empty,
                    );
                    let sender = me.sender.read().await.clone();
                    let joined = match sender {
                        Some(sender) => {
                            span.in_scope(|| dial_span(&format!("{node}@{}", me.endpoint.id().fmt_short()), &channel, &me.channel_id, &[t.addr.id], &[t.addr.id]));
                            sender.join_peers(vec![t.addr.id]).await.is_ok()
                        }
                        None => false,
                    };
                    span.record("joined", joined);
                    span.in_scope(|| tracing::info!("a held member's coverage is stale: it is handed to the channel again"));
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
                let Some(sender) = sender else { continue };
                // The refeed's dial runs in its refeed span: the join span's caller.
                let span = tracing::info_span!("rdm.mesh.connection.update.via-refeed", node = %node, channel = %channel, reason = "no-neighbours-fallback", peers = peers.len(), joined = tracing::field::Empty);
                span.in_scope(|| dial_span(&format!("{node}@{}", me.endpoint.id().fmt_short()), &channel, &me.channel_id, &peers, &peers));
                let joined = sender.join_peers(peers.clone()).await.is_ok();
                span.record("joined", joined);
                span.in_scope(|| tracing::info!("no neighbour: every known peer handed to the channel again"));
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

/// The id of one gossip channel (gossip.md §4): a mesh's topology channel, the node-admin backbone,
/// or an application channel, each derived from the fabric's logical id, never from an address.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChannelId {
    /// A mesh's topology channel, named by the mesh's id: every node of the mesh joins it.
    Mesh {
        /// The fabric.
        fabric: FabricId,
        /// The mesh.
        mesh_id: MeshId,
    },
    /// The fabric's node-admin backbone: node-admins only.
    Backbone {
        /// The fabric.
        fabric: FabricId,
    },
    /// An application channel, named by the name the application gives it (Q362).
    App {
        /// The fabric.
        fabric: FabricId,
        /// The application's channel name.
        name: String,
    },
}

impl ChannelId {
    /// `mesh_id`'s topology channel in `fabric`.
    pub fn mesh(fabric: &FabricId, mesh_id: &MeshId) -> Self {
        Self::Mesh { fabric: fabric.clone(), mesh_id: mesh_id.clone() }
    }
    /// `fabric`'s backbone.
    pub fn backbone(fabric: &FabricId) -> Self {
        Self::Backbone { fabric: fabric.clone() }
    }
    /// The application channel `name` in `fabric`.
    pub fn app(fabric: &FabricId, name: &str) -> Self {
        Self::App { fabric: fabric.clone(), name: name.to_string() }
    }
    /// The iroh-gossip `TopicId` of the channel: the same bytes on every node of the fabric, whatever
    /// RDM version it runs (gossip-by-kind.md D1: no channel id changes).
    pub fn topic(&self) -> TopicId {
        match self {
            Self::Mesh { fabric, mesh_id } => TopicId::from_bytes(*blake3::hash(format!("rafka-mesh-membership:{fabric}:{mesh_id}").as_bytes()).as_bytes()),
            Self::Backbone { fabric } => TopicId::from_bytes(*blake3::hash(format!("rafka-fabric-backbone:{fabric}").as_bytes()).as_bytes()),
            Self::App { fabric, name } => {
                let bytes = rdm_transport::wire::encode(&("rdm.app", fabric, name.as_str())).expect("strings encode");
                TopicId::from(*blake3::hash(&bytes).as_bytes())
            }
        }
    }
}

/// The peers a created node names on its mesh channel (gossip-by-kind.md §5.2, Q376): its launch's
/// seeds, less its launcher when the launcher is of another mesh. A node of another mesh is never on
/// this channel, so a node launched from another mesh is its mesh's first listener, or names the
/// nodes of its own mesh its launch carries. Its backbone and application channels keep every seed.
pub fn mesh_seeds(seeds: &[EndpointAddr], launcher: &iroh::EndpointId, launcher_mesh: &str, mesh: &str) -> Vec<EndpointAddr> {
    seeds.iter().filter(|s| launcher_mesh == mesh || &s.id != launcher).cloned().collect()
}

/// A peer named by its endpoint id alone (`rdm_core::EndpointId`, an endpoint key): its addresses come
/// from what the node holds (`LocationView`). A value that is not an endpoint key is refused by name.
pub fn peer(id: &rdm_core::EndpointId) -> Result<EndpointAddr> {
    let key = id.0.parse::<iroh::PublicKey>().map_err(|e| anyhow::anyhow!("peer endpoint_id {:?} is not an endpoint key: {e}", id.0))?;
    Ok(EndpointAddr::new(key))
}

/// One node's channels: its iroh-gossip handle and endpoint, the topology view its channels fill, the
/// channels it joined and what it publishes on them.
#[derive(Clone)]
pub struct Channels {
    pub(crate) gossip: Gossip,
    pub(crate) endpoint: Endpoint,
    pub(crate) topology_view: TopologyView,
    /// The endpoint's peer-location view: every channel of the node installs its peers' addresses through it.
    pub(crate) location: LocationView,
    /// This birth's author, once supplied: every frame this node authors carries it.
    pub(crate) author: Arc<Mutex<Option<crate::envelope::Birth>>>,
    /// The publisher identity the backbone authors as, once supplied ([`Self::set_author`]).
    pub(crate) publisher: Arc<Mutex<Option<(PublisherId, Option<IncarnationId>)>>>,
    /// The node's mesh topology channel, once joined.
    pub(crate) mesh_channel: Arc<OnceLock<Channel>>,
    /// The node-admin backbone, once joined.
    pub(crate) backbone: Arc<OnceLock<Backbone>>,
    /// The application channels.
    pub(crate) apps: Arc<crate::gossip::Hub>,
    /// The last `digest_seq` this birth published: sender-local, from 1 for this process's birth.
    pub(crate) digest_seq: Arc<std::sync::atomic::AtomicU64>,
    /// This process's CPU and RAM, sampled into every digest it publishes.
    pub(crate) load: Arc<rdm_transport::load::LoadSampler>,
    /// What the forwarder decides and where it goes: this mesh's channel.
    pub(crate) forward_path: ForwardPath,
    /// The forward path's sender end, until the mesh channel it sends on is joined.
    pub(crate) forward_rx: Arc<Mutex<Option<ForwardRx>>>,
}

impl Channels {
    /// The channels of the node whose topology view is `topology_view`, on `endpoint` through `gossip`:
    /// none joined yet.
    pub fn new(gossip: &Gossip, endpoint: &Endpoint, topology_view: &TopologyView) -> Result<Self> {
        let view = topology_view;
        let location = LocationView::new(endpoint, &view.view.current_name())?.with_held_name(view.view.held_name.clone());
        let author: Arc<Mutex<Option<crate::envelope::Birth>>> = Arc::default();
        let (forward_path, forward_rx) = ForwardPath::pair(view.view.forwarder.clone(), Arc::default());
        let apps = crate::gossip::Hub::new(gossip, &view.fabric, author.clone(), view.clock.clone());
        Ok(Self {
            gossip: gossip.clone(),
            endpoint: endpoint.clone(),
            topology_view: view.clone(),
            location,
            author,
            publisher: Arc::default(),
            mesh_channel: Arc::default(),
            backbone: Arc::default(),
            apps,
            digest_seq: Arc::default(),
            load: Arc::new(rdm_transport::load::LoadSampler::for_this_process()),
            forward_path,
            forward_rx: Arc::new(Mutex::new(Some(forward_rx))),
        })
    }

    /// The topology view these channels fill.
    pub fn topology_view(&self) -> &TopologyView {
        &self.topology_view
    }

    /// The node's mesh topology channel. Joined first, by the node's start ([`Self::join`]).
    pub(crate) fn mesh(&self) -> &Channel {
        self.mesh_channel.get().expect("the node joins its mesh's channel before it publishes on it")
    }

    /// The backbone, once this node-admin joined it.
    pub fn backbone(&self) -> Option<Backbone> {
        self.backbone.get().cloned()
    }

    /// The peers a commanded start or join names on `id` (gossip-by-kind.md §5.2, Q376): the
    /// commander, and the peers the channel already knows. The mesh channel names the commander only
    /// when this node's view holds it as a node of its own mesh: a commander the view does not hold
    /// is of another mesh, and a node of another mesh is never on this channel. `None` when `id` does
    /// not name the commander (the mesh channel, a commander of another mesh; an application channel,
    /// whose peers the application names). The commander's address is the one its digest in the view
    /// names, else its key alone, reached over the connection its call came on.
    pub fn with_commander(&self, id: &ChannelId, commander: &iroh::PublicKey) -> Option<Vec<EndpointAddr>> {
        let held = self.topology_view.projected_member_at(&commander.to_string());
        let known: Vec<iroh::EndpointId> = match id {
            ChannelId::Mesh { .. } if held.as_ref().is_some_and(|d| d.node.mesh == self.topology_view.mesh_name()) => self.mesh().peers.lock().unwrap().iter().copied().collect(),
            ChannelId::Backbone { .. } => self.backbone.get()?.channel.peers.lock().unwrap().iter().copied().collect(),
            _ => return None,
        };
        let commander_addr = held.as_ref().and_then(crate::location::gossip_addr).filter(|a| &a.id == commander).unwrap_or_else(|| EndpointAddr::new(*commander));
        Some(std::iter::once(commander_addr).chain(known.into_iter().filter(|k| k != commander).map(EndpointAddr::new)).collect())
    }

    /// Join the channel `id` through `peers` (Q376: `Channel.join(id, peers)`): the first join of an id
    /// subscribes with `peers` as its first neighbours; a later one names more peers on the channel
    /// already held, and starts a held channel that is stopped. Its span names the peers named and the
    /// peers dialled (`rdm.mesh.connection.update.via-join-peers`).
    pub async fn join(&self, id: ChannelId, peers: Vec<EndpointAddr>) -> Result<()> {
        match &id {
            ChannelId::Mesh { mesh_id, .. } => {
                anyhow::ensure!(mesh_id == &self.topology_view.mesh_id, "a node joins its own mesh's channel only: {mesh_id} is not {}", self.topology_view.mesh_id);
                if let Some(channel) = self.mesh_channel.get() {
                    channel.join_peers(peers).await?;
                    return Ok(());
                }
                self.join_mesh(id.topic(), peers).await
            }
            ChannelId::Backbone { .. } => {
                if let Some(backbone) = self.backbone.get() {
                    backbone.join_admins(peers).await;
                    return Ok(());
                }
                let (publisher, _) = self.publisher.lock().unwrap().clone().ok_or_else(|| anyhow::anyhow!("the backbone is joined after the node's author is supplied (Channels::set_author)"))?;
                let backbone = Backbone::join(&self.gossip, &self.endpoint, self, &self.topology_view.view.mesh, publisher, peers).await?;
                let _ = self.backbone.set(backbone);
                Ok(())
            }
            ChannelId::App { name, .. } => {
                let ids = peers.iter().map(|p| p.id).collect();
                self.apps.join(name, ids).await.map_err(|e| anyhow::anyhow!("{e}"))
            }
        }
    }

    /// The application channel `name` this node joined ([`Self::join`] with [`ChannelId::App`]): its
    /// one receiver, handed over once.
    pub async fn app(&self, name: &str) -> Result<crate::gossip::Channel, crate::gossip::ChannelError> {
        self.apps.receiver(name).await
    }

    async fn join_mesh(&self, topic: TopicId, peers: Vec<EndpointAddr>) -> Result<()> {
        let view = &self.topology_view;
        let node = view.view.current_name();
        let (replay_view, replay_fabric, replay_clock, replay_forward_path) = (view.view.clone(), view.fabric.clone(), view.clock.clone(), self.forward_path.clone());
        // A neighbour coming up on this channel (a join, a heal, a refeed) hears what the primary
        // holds: the statuses, and the forwarded full of every source it has published (gossip.md
        // §3.1, §3.3), queued under the forwarder's lock so no delta decided after the replay can
        // pass it on the channel.
        let replay: Arc<dyn Fn() -> Replayed + Send + Sync> = Arc::new(move || {
            if replay_view.primary.load(Ordering::Relaxed) {
                let replay_me = replay_view.current_name();
                let prefix = replay_view.digest_view.statuses.held_frames(&replay_fabric, &replay_me);
                let (frames, full_chunks, bytes) = replay_forward_path.replay(&replay_me, prefix, || replay_clock.now_rafka_ms());
                Replayed { direct: Vec::new(), frames, full_chunks, bytes }
            } else {
                Replayed::default()
            }
        });
        let (v, f, ls, observing) = (view.view.clone(), view.fabric.clone(), self.location.clone(), view.clock.clone());
        let on_frame: Arc<dyn Fn(Frame, &mut crate::receive_order::ReceiveOrder) + Send + Sync> = Arc::new(move |frame: Frame, order: &mut crate::receive_order::ReceiveOrder| {
            let mut digest_fresh = false;
            let carried = match frame.data() {
                Frame::MeshTopology { .. } | Frame::MeshTopologyDelta { .. } => v.take_snapshot(&frame, &f, Side::MeshChannel, order).carried,
                _ => {
                    let (carried, fresh) = v.take_with(&frame, &f, "mesh-channel");
                    digest_fresh = fresh;
                    // A member's clock follows its current mesh primary's heartbeat, judged on the
                    // digest as its view holds it: the primary's status is its own word, in its
                    // projection, never on its heartbeat.
                    if let (Frame::Digest { .. }, Some(held)) = (frame.data(), carried.first()) {
                        let me_now = v.current_name();
                        let verdict = crate::discipline::from_mesh_primary_digest(&v.mesh, &me_now, v.primary.load(Ordering::Relaxed), &v.digest_view.primaries, held, fresh);
                        crate::discipline::offer(&observing, &me_now, "mesh-primary-digest", verdict);
                    }
                    carried
                }
            };
            for d in &carried {
                ls.register(d);
            }
            if let Frame::Digest { digest } = frame.data() {
                tracing::info_span!(
                    "rdm.mesh.topology.update.via-digest-receipt",
                    node = %v.current_name(), node_id = %digest.node.node_id,
                    incarnation_id = %digest.node.incarnation, digest_seq = digest.digest_seq,
                    endpoint_id = %digest.node.endpoint_id.0, applied = digest_fresh,
                    addresses_registered = carried.len(),
                ).in_scope(|| tracing::info!("digest receipt finished classification and address registration"));
            }
        });
        let (digest_view, own_mesh, target_view) = (view.digest_view.clone(), view.view.mesh.clone(), view.view.clone());
        let targets: Arc<dyn Fn() -> Held + Send + Sync> =
            Arc::new(move || {
                let me_name = target_view.current_name();
                held_view(&digest_view, |d| d.node.mesh == own_mesh && d.node.node_id.to_string() != me_name)
            });

        let channel = Channel::join(&self.gossip, &self.endpoint, topic, view.fabric.as_str(), &node, &format!("mesh:{}", view.view.mesh), peers, self.location.clone(), view.view.order.clone(), on_frame, targets, replay, self.author.clone(), view.clock.clone()).await?;
        let sink_channel = channel.clone();
        let sink: crate::forward_path::Sink = Arc::new(move |f: Frame| {
            let channel = sink_channel.clone();
            Box::pin(async move { channel.broadcast(&f).await })
        });
        if let Some(rx) = self.forward_rx.lock().unwrap().take() {
            self.forward_path.spawn_sender(rx, sink);
        }
        let _ = self.mesh_channel.set(channel);
        Ok(())
    }
    /// The process's iroh endpoint this topology view rides: for read-only observations
    /// (`rdm_transport::iroh_obs`), never for sending.
    pub fn endpoint(&self) -> Endpoint {
        self.mesh().endpoint.clone()
    }

    /// The endpoint's peer-location view: every other channel this process joins on the endpoint
    /// installs its peers' addresses through it.
    pub fn location_view(&self) -> LocationView {
        self.mesh().location_view.clone()
    }

    /// Testkit-only one-shot incoming chunk loss, absent from production.
    #[cfg(feature = "testkit-gossip")]
    pub async fn drop_app_chunk_once(&self, kind: &str, index: usize) -> anyhow::Result<()> { self.apps.drop_chunk_once(kind, index).await }

    /// The exact author birth supplied by this process, when publication has bound it.
    pub fn author_birth(&self) -> Option<crate::envelope::Birth> { self.author.lock().unwrap().clone() }

    /// Hold the publisher the backbone publishes as, alone, for [`Self::join`] with
    /// [`ChannelId::backbone`]: what the backbone's frames are authored by. [`Self::set_author`]
    /// holds it with the node's own birth; a cell whose backbone publishes under another identity
    /// than its view's own holds it with this, its view's own birth left unset.
    pub fn set_publisher(&self, publisher: &PublisherId) {
        *self.publisher.lock().unwrap() = Some((publisher.clone(), None));
    }

    /// Supply this process's owned author birth; never resolve it from a descriptive name.
    pub fn set_author(&self, author: &PublisherId, predecessor: Option<IncarnationId>) {
        *self.topology_view.digest_view.own.lock().unwrap() = Some(author.node_id.to_string());
        *self.topology_view.digest_view.own_predecessor.lock().unwrap() = predecessor.clone();
        *self.topology_view.digest_view.observer_name.lock().unwrap() = author.node_id.to_string();
        let birth = crate::envelope::Birth::publisher(&self.topology_view.fabric, author);
        *self.author.lock().unwrap() = Some(birth.clone());
        *self.publisher.lock().unwrap() = Some((author.clone(), predecessor.clone()));
        self.topology_view.view.forwarder.lock().unwrap().set_author(birth);
    }

    /// Hold the digest a birth presented at its join (`JoinNode`), and register where it is. A join
    /// digest carries no status: an own-mesh member is held as heard, with the status this view holds
    /// for it and none authored from the digest ([`DigestView::record_joined`]); a peer mesh's member
    /// is topology, its held status kept.
    pub fn learn(&self, d: MeshDigest, via: &'static str) {
        if d.fabric_id != self.topology_view.fabric {
            return;
        }
        self.mesh().register(&d);
        let taken = if d.node.mesh == self.topology_view.view.mesh {
            self.topology_view.digest_view.record_joined(d.clone())
        } else {
            let held = self.topology_view.digest_view.get(d.node.node_id.as_str()).filter(|(h, _)| h.node.incarnation == d.node.incarnation).map(|(h, _)| h.status);
            self.topology_view.digest_view.record_topology(MeshDigest { status: held.unwrap_or(rdm_core::MemberStatus::Pending), ..d.clone() })
        };
        if taken {
            self.topology_view.view.note(&d, via);
        }
    }

    /// Broadcast this member's digest on its mesh channel (and record it). The heartbeat is
    /// stamped here, once, for every caller: `digest_seq` is the next of this birth's own
    /// sequence (from 1), `emitted_at_rafka_ms` is the composed clock's reading, and `node_state` is
    /// the liveness it self-publishes. The order a receiver acts on is the sequence, whatever the
    /// clock reads.
    pub async fn publish(&self, d: &MeshDigest) -> Result<()> {
        {
            let birth = crate::envelope::Birth { fabric: d.fabric_id.clone(), node: d.node.node_id.clone(), incarnation: d.node.incarnation.clone() };
            let mut author = self.author.lock().unwrap();
            anyhow::ensure!(author.as_ref().is_none_or(|held| held == &birth), "digest birth differs from the supplied author birth");
            *author = Some(birth);
        }
        *self.topology_view.digest_view.own.lock().unwrap() = Some(d.node.node_id.to_string());
        let mut d = self.topology_view.reported(d);
        d.digest_seq = self.digest_seq.fetch_add(1, Ordering::SeqCst) + 1;
        d.emitted_at_rafka_ms = self.topology_view.clock.now_rafka_ms();
        // The liveness this birth self-publishes (node-identity-and-state.md §4), from its own word
        // about itself; its status never travels on its heartbeat.
        d.node_state = rdm_core::NodeState::self_published(d.status);
        d.load = Some(self.load.sample());
        // The primaries this birth is, as its view holds their records: its digest is their normal
        // announcement (gossip.md "A primary change on gossip").
        self.topology_view.digest_view.recorded_primaries(&mut d);
        d.gossip = Some(rdm_core::GossipStats {
            heard: self.topology_view.digest_view.current(self.topology_view.digest_view.staleness_floor()).len() as u32,
            neighbours: self.mesh().neighbors.lock().unwrap().len() as u32,
            frames_sent: self.mesh().frames_sent.load(Ordering::Relaxed),
            frames_received: self.mesh().frames_received.load(Ordering::Relaxed),
        });
        self.topology_view.digest_view.record(d.clone());
        self.mesh().broadcast(&Frame::Digest { digest: d }).await
    }

    /// Broadcast a frame onto this mesh's channel (a forward).
    pub async fn forward(&self, f: &Frame) -> Result<()> {
        self.mesh().broadcast(f).await
    }

    /// Leave this mesh's gossip channel: this node is a neighbour of no one on it and sends and hears
    /// nothing there until [`TopologyView::start`]. Resolves once the channel is left.
    pub async fn stop(&self) {
        self.apps.suspend().await;
        self.mesh().stop().await;
    }

    /// Join this mesh's gossip channel again through the peers it knows. Resolves once the channel is
    /// re-subscribed.
    pub async fn start(&self) {
        self.mesh().start().await;
        // Back on the channel, this node heard nothing while it was off it: every member's silence is
        // measured from now, never from before the stop (node-recovery.md step 1: the window already
        // running is the durable silence rows', which the tick reads). On the channel again it hears its
        // members at once: the reopen dialled them (Q88).
        self.topology_view.digest_view.restart_silence();
        if let Err(error) = self.apps.resume().await {
            tracing::info_span!("rdm.mesh.channel.reject.via-subscribe", error = %error).in_scope(|| tracing::warn!("application channels could not rejoin"));
        }
    }

    /// Resolves once this node's mesh channel has a gossip neighbour (at once when it has one). For a
    /// test of gossip itself, which needs neighbours before it sends: no start or other step waits on
    /// gossip, which is eventual and unordered.
    pub async fn wait_for_neighbour(&self) {
        let mut changed = self.mesh().neighbors_changed.subscribe();
        loop {
            changed.borrow_and_update();
            if !self.mesh().neighbors.lock().unwrap().is_empty() {
                return;
            }
            if changed.changed().await.is_err() {
                return;
            }
        }
    }

    /// The active gossip neighbours on this node's mesh channel.
    pub fn neighbours(&self) -> usize {
        self.mesh().neighbors.lock().unwrap().len()
    }

    /// Publish a lifecycle event this node authored on its own mesh channel,
    /// applying it to its own view first. A `NodeDeleted` it authored is a deletion it executed: its
    /// mesh's topology carries it on the backbone for the retention, so the deleted birth's own mesh
    /// takes it when the birth was in a peer mesh (gossip.md §6.1).
    pub async fn publish_lifecycle(&self, f: &Frame) -> Result<()> {
        if let Frame::NodeDeleted { op, forwarded_by: None } = f {
            self.topology_view.digest_view.note_executed_deletion(op);
        }
        let authored = self.mesh().authored(f)?;
        let f = &authored;
        let _ = self.topology_view.view.take(f, &self.topology_view.fabric, "mesh-channel");
        self.mesh().broadcast(f).await
    }

    /// One chunk of a topology read from another node. The mesh installs atomically once every
    /// chunk of its snapshot is held, through the same receiver the Mesh channel's snapshots
    /// install into; a source it ends the desynchronization of is resumed. Every member the
    /// snapshot carries is held as heard and its location registered.
    pub fn take_read_chunk(&self, chunk: Chunk, answerer_mesh: &str) -> Taken {
        let side = if answerer_mesh == self.topology_view.view.mesh { Side::Read } else { Side::ReadPeer };
        let own_digests = (side == Side::Read && chunk.mesh == self.topology_view.view.mesh).then(|| chunk.digests.clone());
        let taken = self.topology_view.view.take_read_chunk(chunk, side, &self.topology_view.fabric);
        if let Taken::Installed(i) = &taken {
            for d in i.full.digests().into_iter().filter(|d| d.fabric_id == self.topology_view.fabric) {
                self.mesh().register(&d);
            }
            // A peer source installed from a topology read is a full this node holds: while it holds
            // its Mesh's forwarder role, it goes into the Mesh as a backbone install would (gossip.md
            // §3.3); without the role, its first full carries it (`Backbone::forward_fulls`).
            if side == Side::ReadPeer && i.mesh != self.topology_view.view.mesh {
                let me = self.topology_view.view.current_name();
                let _ = self.forward_path.source_with_clock(&me, &i.mesh, &i.publisher, i.topology_version, &i.full, || self.topology_view.clock.now_rafka_ms());
            }
        } else if let Some(digests) = own_digests {
            for d in digests.into_iter().filter(|d| d.fabric_id == self.topology_view.fabric && d.node.mesh == self.topology_view.view.mesh && self.topology_view.digest_view.get(d.node.node_id.as_str()).is_some()) {
                self.mesh().register(&d);
            }
        }
        taken
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
    /// behind its stop gate, so a stopped admin tops up nothing and a gap signalled while it was stopped
    /// is topped up once it runs again.
    pub fn top_ups(&self, fetch: TopUpFetch) -> impl std::future::Future<Output = ()> + Send + 'static {
        let me = self.clone();
        async move {
            let pacing = rdm_transport::delivery::Pacing::from_cadences();
            let mut again = 0u32;
            loop {
                // One attempt per signal: a delta heard on a desynchronized source signals again,
                // so a top-up that failed is attempted when the primary next speaks. A pull that
                // installed a copy older than a delta dropped while it was in flight pulls again
                // (gossip.md §3.3, Q130), on the one backoff rule.
                if again == 0 {
                    me.topology_view.view.desync.notified().await;
                } else {
                    tokio::time::sleep(pacing.after(again)).await;
                }
                let gaps = me.topology_view.desynced_sources();
                again = if !gaps.is_empty() && me.top_up(&fetch, gaps).await { again + 1 } else { 0 };
            }
        }
    }

    /// One pull of `gaps`: `true` when it installed a copy of a source older than a delta that
    /// source dropped while the pull was in flight, so the source is still behind.
    pub(crate) async fn top_up(&self, fetch: &TopUpFetch, gaps: Vec<(String, Gap)>) -> bool {
        let meshes = gaps.iter().map(|(m, _)| m.as_str()).collect::<Vec<_>>().join(",");
        let reasons = gaps.iter().map(|(m, g)| format!("{m}:{g}")).collect::<Vec<_>>().join(",");
        let node = self.topology_view.view.current_name();
        let primary = match self.topology_view.mesh_primary() {
            Ok(primary) => primary,
            Err(no) => {
                tracing::info_span!("rdm.mesh.entry.reject.via-top-up", node = %node, meshes = %meshes, reasons = %reasons, reason = no.reason(), primary = %no.primary().map(|h| h.to_string()).unwrap_or_default(), detail = %no)
                    .in_scope(|| tracing::info!("this Mesh's current primary cannot be addressed: nothing to top up from"));
                return false;
            }
        };
        // The Mesh's primary has no primary to top up from: it serves its own mesh from what it
        // publishes and every peer mesh from the backbone, never from its mesh-channel sources.
        if self.topology_view.digest_view.own.lock().unwrap().as_deref() == Some(primary.node.node_id.as_str()) {
            tracing::info_span!("rdm.mesh.entry.reject.via-top-up", node = %node, meshes = %meshes, reasons = %reasons, mesh_primary_path = %primary.node.path, mesh_primary_name = %primary.node.name, reason = "this-node-is-the-mesh-primary")
                .in_scope(|| tracing::info!("this node is its Mesh's primary: it has no primary to top up from, and it serves no source from its mesh channel"));
            return false;
        }
        let names: Vec<String> = gaps.iter().map(|(m, _)| m.clone()).collect();
        match fetch(self.clone(), primary.clone(), names).await {
            Ok(done) => {
                let still: Vec<String> = self.topology_view.desynced_sources().into_iter().map(|(m, _)| m).collect();
                let behind = done.installed.iter().any(|(m, _)| still.contains(m));
                tracing::info_span!(
                    "rdm.mesh.entry.update.via-top-up",
                    node = %node,
                    meshes = %meshes,
                    reasons = %reasons,
                    mesh_primary_path = %primary.node.path, mesh_primary_name = %primary.node.name,
                    served_by_path = %primary.node.path, served_by_name = %primary.node.name,
                    installed = done.installed.iter().map(|(m, v)| format!("{m}@{v}")).collect::<Vec<_>>().join(","),
                    still_desynced = still.join(","),
                    pulls_again = behind,
                )
                .in_scope(|| tracing::info!("topped up from this Mesh's own primary"));
                behind
            }
            Err(e) => {
                tracing::info_span!("rdm.mesh.entry.reject.via-top-up", node = %node, meshes = %meshes, reasons = %reasons, mesh_primary_path = %primary.node.path, mesh_primary_name = %primary.node.name, reason = "pull-refused", error = %e)
                    .in_scope(|| tracing::info!("the Mesh's primary did not answer the top-up"));
                false
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
                if !*me.mesh().stopped.borrow() {
                    let d = digest();
                    let _ = me.publish(&d).await;
                    if last_heartbeat.is_none_or(|at| at.elapsed() >= HEARTBEAT_EVERY) {
                        last_heartbeat = Some(Instant::now());
                        me.heartbeat(&d);
                    }
                }
                tokio::time::sleep(every).await;
            }
        })
    }

    /// One heartbeat span, a root of its own: this node is alive and publishing, how many other
    /// members its view holds as heard, and what its clock reads against the host's wall clock.
    pub(crate) fn heartbeat(&self, d: &MeshDigest) {
        let name = self.topology_view.held_name(&d.node.location());
        let peer_count = self.topology_view.digest_view.current(self.topology_view.digest_view.staleness_floor()).iter().filter(|m| m.node.node_id != d.node.node_id).count() as u64;
        let wall_time_ms = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|t| t.as_millis() as i64).unwrap_or(0);
        let rafka_time_ms = self.topology_view.clock.now_rafka_ms() as i64;
        tracing::info_span!(
            parent: None,
            "rdm.mesh.node.update.via-heartbeat",
            node_path = %name.path, node_name = %name.name,
            node_id = %d.node.node_id,
            mesh = %d.node.mesh,
            peer_count,
            wall_time_ms,
            rafka_time_ms,
            clock_skew_ms = rafka_time_ms - wall_time_ms,
            digest_seq = self.digest_seq.load(Ordering::SeqCst),
        )
        .in_scope(|| tracing::info!("heartbeat"));
    }

}
