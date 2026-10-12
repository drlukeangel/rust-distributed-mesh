//! The fabric control projection of Build state (PRD §1.4, §7;
//! mesh-control-plane.md §3–§4.1).
//!
//! Every node-admin of a fabric holds the same Build facts: each fact an admin
//! appends (intent, attempt claim, step receipt, attempt receipt, forget) is
//! applied to its own local log (`builds.storage`) and broadcast once on the
//! fabric's Build topic over iroh-gossip; every other admin absorbs it. A
//! successor therefore continues a Build from its own live projection, never
//! from the executing admin's disk.
//!
//! An admin that gains a neighbour on the topic (one joining, or one
//! reconnecting after a partition) sends it the facts of every active Build
//! it holds, to its direct neighbours only (`broadcast_neighbors`): a fact
//! broadcast before a member was connected is otherwise never delivered to
//! it, and a successor that misses a Build cannot continue it.
//!
//! Facts fold deterministically and absorb idempotently: intents and claims
//! are insert-and-fail, receipts deduplicate. Split-primary safety comes from
//! reconciliation over pinned intents and idempotent operation keys, not from
//! the claim alone (mesh-control-plane.md §4.1).

use crate::build::BuildId;
use crate::accepted::AcceptedStore;
use crate::build_state::{
    AttemptOpened, BuildAccepted, BuildAttemptClaim, BuildAttemptReceipt, BuildFact, BuildProjection, BuildStateAdapter,
    BuildStateError, BuildStepReceipt, ClaimOutcome, LocalBuildLog,
};
use crate::fabric_storage::FabricRecord;
use futures_lite::StreamExt as _;
use iroh::EndpointAddr;
use iroh_gossip::api::{Event, GossipSender};
use iroh_gossip::net::Gossip;
use iroh_gossip::proto::TopicId;
use std::sync::Arc;
use tracing::Instrument as _;

/// One message on the Build topic, one postcard frame (`rafka_mesh_transport::wire`). The nonce
/// makes every send distinct, so a catch-up resend is never taken for a message already seen.
///
/// `fabric` is the Fabric record (`Fabric.build_id`): Fabric control state of
/// its own, sent alone when the pointer moves and to a new neighbour, never
/// inside a run of Build facts. A neighbour's catch-up hands it the record and
/// the facts of the Build it names and of every active Build. Facts and the Fabric record
/// travel in their wire shapes (`crate::wire`), never the JSON-shaped domain types.
pub struct BuildMessage {
    /// The message's nonce.
    pub nonce: u64,
    /// The Build facts the message carries.
    pub facts: Vec<BuildFact>,
    /// The Fabric record the message carries.
    pub fabric: Option<FabricRecord>,
    /// A fabric shutdown in force: Fabric control state, sent alone when initiated and to a new
    /// neighbour first (fabric-mesh-lifecycle.md §11.1). Never a Build fact.
    pub shutdown: Option<crate::fabric_storage::FabricShutdown>,
}

impl BuildMessage {
    /// This message as one gossip payload; the payload is not bounded here (see [`encode`]).
    pub fn to_bytes(&self) -> Result<bytes::Bytes, BuildStateError> {
        let wire = crate::wire::WireMessage::of(self).map_err(|e| BuildStateError::Unencodable(e.to_string()))?;
        rafka_mesh_transport::wire::encode(&wire).map(Into::into).map_err(|e| BuildStateError::Unencodable(e.to_string()))
    }

    /// The message a gossip payload carries, or why it is not one.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, rafka_mesh_transport::wire::WireError> {
        let wire: crate::wire::WireMessage = rafka_mesh_transport::wire::decode(bytes)?;
        wire.into_message().map_err(|e| rafka_mesh_transport::wire::WireError::new(format!("a Build fact this build does not read: {e}")))
    }
}

fn encode_shutdown(s: &crate::fabric_storage::FabricShutdown) -> Result<bytes::Bytes, BuildStateError> {
    BuildMessage { nonce: rand::random(), facts: Vec::new(), fabric: None, shutdown: Some(s.clone()) }.to_bytes()
}

fn encode_fabric(r: &FabricRecord) -> Result<bytes::Bytes, BuildStateError> {
    BuildMessage { nonce: rand::random(), facts: Vec::new(), fabric: Some(r.clone()), shutdown: None }.to_bytes()
}

pub use rafka_mesh_transport::chunking::{GOSSIP_FRAME_LIMIT, MAX_MESSAGE_BYTES};

fn encode(facts: Vec<BuildFact>) -> Result<bytes::Bytes, BuildStateError> {
    let bytes = BuildMessage { nonce: rand::random(), facts, fabric: None, shutdown: None }.to_bytes()?;
    if bytes.len() > MAX_MESSAGE_BYTES {
        return Err(BuildStateError::Io(format!(
            "a Build message of {} bytes exceeds the gossip payload limit of {MAX_MESSAGE_BYTES} (frame limit {GOSSIP_FRAME_LIMIT})",
            bytes.len()
        )));
    }
    Ok(bytes.into())
}

/// `facts` packed, in order, into messages that each fit the gossip limit.
/// A fact that fits no message on its own is left out and named.
pub fn encode_chunks(facts: Vec<BuildFact>) -> (Vec<bytes::Bytes>, Vec<BuildStateError>) {
    // Each encoding draws a fresh nonce, whose length varies: a batch is sent as the bytes that
    // were measured to fit, never encoded a second time.
    let (runs, refused) = rafka_mesh_transport::chunking::pack_in_order(facts, |batch| encode(batch.to_vec()));
    (runs.into_iter().map(|(_, b)| b).collect(), refused)
}

/// A broadcast on the Build topic that failed, named: a send is durability and recovery evidence
/// (node-rpc-envelope.md "Build carrier": progress waits on no gossip), so its failure fails nothing
/// that was committed locally; the neighbour-up catch-up and `FetchBuildFacts` carry what it held.
/// `fact` says which send (`hand-over` of a yielded seat, `neighbour-up` catch-up), `sent_by` who.
fn span_broadcast_refused(sent_by: &str, fact: &'static str, detail: String) {
    tracing::info_span!("rdm.node_admin.build.reject.via-broadcast-refused", sent_by = %sent_by, fact, detail = %detail)
        .in_scope(|| tracing::info!("a Build topic send failed: the neighbour-up catch-up and FetchBuildFacts carry what it held"));
}

/// Every fact [`encode_chunks`] left out, named under its own span: a fact with no wire shape, or
/// one that fits no gossip message. `sent_by` says which send left it out (the fabric's
/// neighbour-up catch-up, or a node-admin's seat hand-over).
fn span_refused(sent_by: &str, refused: &[BuildStateError]) {
    for e in refused {
        match e {
            BuildStateError::Unencodable(_) => tracing::info_span!("rdm.node_admin.build.reject.via-unencodable-fact", sent_by = %sent_by, detail = %e)
                .in_scope(|| tracing::info!("a Build fact has no wire shape")),
            _ => tracing::info_span!("rdm.node_admin.build.reject.via-oversized-fact", sent_by = %sent_by, detail = %e)
                .in_scope(|| tracing::info!("a Build fact does not fit one gossip message")),
        }
    }
}

/// The facts of every active Build `local` holds, and of `also` (the Build the pointer names),
/// in append order.
async fn active_facts(local: &dyn LocalBuildLog, also: Option<BuildId>) -> Vec<BuildFact> {
    let mut active: std::collections::BTreeSet<BuildId> = match local.list_active().await {
        Ok(a) => a.into_iter().map(|b| b.build_id).collect(),
        Err(_) => return Vec::new(),
    };
    active.extend(also);
    match local.facts().await {
        Ok(f) => f.into_iter().filter(|f| active.contains(f.build_id())).collect(),
        Err(_) => Vec::new(),
    }
}

fn refeed(
    fabric: String,
    sender: Arc<tokio::sync::RwLock<GossipSender>>,
    known: Arc<std::sync::Mutex<Vec<iroh::EndpointId>>>,
    neighbors: Arc<std::sync::Mutex<std::collections::BTreeSet<iroh::EndpointId>>>,
) {
    use rafka_mesh_transport::membership::backbone_gossip_interval;
    tokio::spawn(async move {
        // As the membership channels (PRD §6.2): a bounded join attempt one gossip interval after
        // the topic was left alone, backed off (doubling, at most one floor apart) while it holds.
        let mut alone_since: Option<std::time::Instant> = None;
        let mut backoff = backbone_gossip_interval();
        loop {
            tokio::time::sleep(backbone_gossip_interval()).await;
            if !neighbors.lock().unwrap().is_empty() {
                alone_since = None;
                backoff = backbone_gossip_interval();
                continue;
            }
            let since = *alone_since.get_or_insert_with(std::time::Instant::now);
            if since.elapsed() < backoff {
                continue;
            }
            backoff = rafka_mesh_transport::membership::refeed_backoff(backoff);
            let mut peers = known.lock().unwrap().clone();
            peers.sort();
            peers.dedup();
            if peers.is_empty() {
                continue;
            }
            let s = sender.read().await.clone();
            let joined = s.join_peers(peers.clone()).await.is_ok();
            tracing::info_span!("rdm.mesh.connection.update.via-refeed", channel = "builds", fabric = %fabric, peers = peers.len(), joined)
                .in_scope(|| tracing::info!("no neighbour: every known peer handed to the Build topic again"));
            alone_since = Some(std::time::Instant::now());
        }
    });
}

/// The fabric's Build topic.
pub(crate) fn build_topic(fabric: &rafka_mesh_entity::FabricId) -> TopicId {
    TopicId::from_bytes(*blake3::hash(format!("rafka-fabric-builds:{fabric}").as_bytes()).as_bytes())
}

/// A node-admin's Build state: its own copy of the fabric's Build facts.
pub struct FabricBuildStateAdapter {
    local: Arc<dyn LocalBuildLog>,
    sender: Arc<tokio::sync::RwLock<GossipSender>>,
    /// Every peer known on the Build topic (seeds, neighbours, admins heard).
    known: Arc<std::sync::Mutex<Vec<iroh::EndpointId>>>,
    /// The admins joined while they stayed live.
    joined: std::sync::Mutex<std::collections::BTreeSet<iroh::EndpointId>>,
    /// The authority gate every fabric-primary mutation passes (ruling R-A2).
    authority: Authority,
}

/// What the committed fabric seat record says of this birth's Build authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SeatRecord {
    /// The record names this birth, names none, or names a birth proven gone: it ends nothing.
    Open {
        /// The path.name of this birth when the record names it (from the digests held); `None`
        /// when the record names none or a birth proven gone.
        holder: Option<String>,
    },
    /// The record names another live birth: this birth's authority ended at that record's epoch.
    Moved {
        /// The record, in words.
        by: String,
        /// The path.name of the birth the record names, when a digest of it is held.
        to: Option<String>,
    },
}

/// Where a Build log reads its authority: the committed seat record, and a receiver that wakes each
/// time a seat record is held.
#[derive(Clone)]
pub struct SeatSource {
    /// The record's answer for this birth, read now.
    pub read: Arc<dyn Fn() -> SeatRecord + Send + Sync>,
    /// Ticks whenever a seat record is held (`SeatBook::subscribe`); none for a log with no seat.
    pub changed: Option<tokio::sync::watch::Receiver<u64>>,
}

impl SeatSource {
    /// A Build log no seat record governs (a fixture with no membership).
    pub fn unrecorded() -> Self {
        Self { read: Arc::new(|| SeatRecord::Open { holder: None }), changed: None }
    }
}

/// The gate of the fabric-primary's mutations: one lock, held from the decision through its
/// durable commit and its broadcast, and the committed fabric seat record read under it. A seat
/// commit on this admin (the handover door's, TakeSeats') holds the same lock across the record's
/// move, so a decision is either wholly committed before the record moves or refused by it. The
/// record is the fence (node-rpc-envelope.md TakeSeats): nothing else is written to fence the log.
pub struct Authority {
    node: String,
    decisions: Arc<tokio::sync::Mutex<()>>,
    /// The committed seat record this birth's authority is read from.
    seat: SeatSource,
}

impl Authority {
    /// A gate for `node`'s Build log, its authority read from `seat`.
    pub fn new(node: String, seat: SeatSource) -> Self {
        Self { node, decisions: Arc::default(), seat }
    }

    /// Resolves once the seat record ends this birth's authority (at once when it already has;
    /// never for a log no seat record governs).
    pub async fn fence_set(&self) {
        let Some(mut changed) = self.seat.changed.clone() else { return std::future::pending().await };
        loop {
            if matches!((self.seat.read)(), SeatRecord::Moved { .. }) {
                return;
            }
            if changed.changed().await.is_err() {
                return std::future::pending().await;
            }
        }
    }

    /// Enter the gate for one mutation: refused by name once the seat record names another live
    /// birth.
    pub async fn enter(&self) -> Result<tokio::sync::MutexGuard<'_, ()>, BuildStateError> {
        self.enter_naming().await.map(|(held, _)| held)
    }

    /// [`Self::enter`], with the holder an open record names (`SeatRecord::Open::holder`), read
    /// under the same lock.
    pub async fn enter_naming(&self) -> Result<(tokio::sync::MutexGuard<'_, ()>, Option<String>), BuildStateError> {
        let held = self.decisions.lock().await;
        match (self.seat.read)() {
            SeatRecord::Open { holder } => Ok((held, holder)),
            SeatRecord::Moved { by, to } => Err(BuildStateError::Fenced { node: self.node.clone(), by, to }),
        }
    }

    /// The lock a seat commit on this admin holds across the record's move.
    pub fn decisions(&self) -> Arc<tokio::sync::Mutex<()>> {
        self.decisions.clone()
    }
}

impl FabricBuildStateAdapter {
    /// Join `fabric`'s Build topic through `peers` (the other node-admins), as
    /// bootstrap hints: it returns at once, connected or not.
    ///
    /// `accepted` is this admin's `Fabric.build_id` holder: every Fabric record heard on the
    /// topic is offered to it, and a new neighbour is handed the one it holds first. `node` names
    /// this admin in evidence.
    pub async fn join(
        gossip: &Gossip,
        book: &rafka_mesh_transport::membership::LocationBook,
        fabric: &rafka_mesh_entity::FabricId,
        peers: Vec<EndpointAddr>,
        local: Arc<dyn LocalBuildLog>,
        accepted: Arc<AcceptedStore>,
        shutdown: Arc<crate::shutdown::ShutdownControl>,
        node: String,
        seat: SeatSource,
        catch_up_seam: Option<Arc<dyn crate::wiring::CatchUpSeam>>,
    ) -> Result<Self, BuildStateError> {
        let io = |e: String| BuildStateError::Io(format!("fabric Build topic {fabric}: {e}"));
        let node_name = node.clone();
        for p in &peers {
            book.fill(p);
        }
        // Seeds are bootstrap hints, never a start barrier: a recorded seed can be dead (an admin
        // that restarted binds a new mesh port), and a dead seed must not keep this admin from the
        // point where it can take part in recovery. Subscribe locally; the seeds are dialled as the
        // topic's first peers and every admin heard later is joined through `join_admins`/refeed.
        let ids = peers.iter().map(|p| p.id).collect();
        let topic = gossip.subscribe(build_topic(fabric), ids).await.map_err(|e| io(e.to_string()))?;
        let (sender, mut receiver) = topic.split();
        let sender = Arc::new(tokio::sync::RwLock::new(sender));
        let (absorb, shared, store, held_shutdown) = (local.clone(), sender.clone(), accepted.clone(), shutdown.clone());
        let (fabric_name, gossip, topic_id) = (fabric.to_string(), gossip.clone(), build_topic(fabric));
        let seed_ids: Vec<iroh::EndpointId> = peers.iter().map(|p| p.id).collect();
        let known_peers: Arc<std::sync::Mutex<Vec<iroh::EndpointId>>> = Arc::new(std::sync::Mutex::new(seed_ids.clone()));
        let neighbors: Arc<std::sync::Mutex<std::collections::BTreeSet<iroh::EndpointId>>> = Arc::default();
        refeed(fabric.to_string(), shared.clone(), known_peers.clone(), neighbors.clone());
        let known_peers_handle = known_peers.clone();
        let seed_ids_set: std::collections::BTreeSet<iroh::EndpointId> = seed_ids.iter().copied().collect();
        tokio::spawn(async move {
            loop {
                // Receive until the subscription lags or ends; iroh-gossip
                // closes a lagging subscriber and expects it to be re-opened.
                let reason = loop {
                    match receiver.next().await {
                        Some(Ok(Event::Received(m))) => match BuildMessage::from_bytes(&m.content) {
                            Ok(w) => {
                                let heard_fabric = w.fabric;
                                if let Some(sd) = w.shutdown {
                                    if let Err(e) = held_shutdown.learn(sd, "gossip", &m.delivered_from.to_string()).await {
                                        tracing::info_span!("rdm.node_admin.fabric.reject.via-shutdown-unpersisted", node = %node, error = %e)
                                            .in_scope(|| tracing::info!("a fabric shutdown could not be persisted"));
                                    }
                                }
                                absorb.absorb_facts(&w.facts).await;
                                if let Some(r) = heard_fabric {
                                    store.learn(r, &*absorb, &m.delivered_from.to_string()).await;
                                } else if !w.facts.is_empty() {
                                    store.resolve_wanted(&*absorb).await;
                                }
                            }
                            Err(e) => tracing::info_span!("rdm.node_admin.build.reject.via-undecodable-fact", fabric = %fabric_name, sender = %m.delivered_from.fmt_short(), bytes = m.content.len(), error = %e)
                                .in_scope(|| tracing::info!("a Build fact from the fabric does not decode")),
                        },
                        Some(Ok(Event::NeighborUp(peer))) => {
                            known_peers.lock().unwrap().push(peer);
                            neighbors.lock().unwrap().insert(peer);
                            if catch_up_seam.as_ref().is_some_and(|s| s.withholds(&node, &peer.to_string())) {
                                tracing::info_span!("rdm.node_admin.build.reject.via-catch-up-withheld", node = %node, fabric = %fabric_name, peer = %peer)
                                    .in_scope(|| tracing::info!("the executable withholds this neighbour's catch-up: nothing is sent to it"));
                                continue;
                            }
                            // Sent from its own task: the receive loop never waits on the actor.
                            let (absorb, shared, fabric_name) = (absorb.clone(), shared.clone(), fabric_name.clone());
                            let (record, pointer) = (store.record().await.ok().flatten(), store.build_id().await);
                            let in_force = held_shutdown.held();
                            let sent_by = node.clone();
                            tokio::spawn(async move {
                                let catch_up = format!("neighbour-up catch-up of fabric {fabric_name} by {sent_by} for {peer}");
                                let send = async |bytes: bytes::Bytes, what: &str| {
                                    if let Err(e) = shared.read().await.clone().broadcast_neighbors(bytes).await {
                                        span_broadcast_refused(&sent_by, "neighbour-up", format!("sending {what} in the {catch_up}: {e}"));
                                    }
                                };
                                // A shutdown in force first: the neighbour comes up frozen.
                                match in_force.as_ref().map(encode_shutdown) {
                                    Some(Ok(sd)) => send(sd, "the fabric shutdown in force").await,
                                    Some(Err(e)) => span_refused(&catch_up, &[e]),
                                    None => {}
                                }
                                // The Fabric record first, as its own message; then the facts of the
                                // Build it names and of every active Build.
                                match record.as_ref().map(encode_fabric) {
                                    Some(Ok(r)) => send(r, "the Fabric record").await,
                                    Some(Err(e)) => span_refused(&catch_up, &[e]),
                                    None => {}
                                }
                                let facts = active_facts(&*absorb, pointer.clone()).await;
                                let _span = tracing::info_span!(
                                    "rdm.node_admin.build.update.via-neighbor-up",
                                    fabric = %fabric_name,
                                    peer = %peer,
                                    facts = facts.len(),
                                );
                                let (messages, refused) = encode_chunks(facts);
                                span_refused(&catch_up, &refused);
                                for bytes in messages {
                                    send(bytes, "a chunk of Build facts").await;
                                }
                            });
                        }
                        Some(Ok(Event::NeighborDown(peer))) => {
                            neighbors.lock().unwrap().remove(&peer);
                        }
                        Some(Ok(Event::Lagged)) => break "lagged: events were dropped".to_string(),
                        Some(Err(e)) => break format!("subscription error: {e}"),
                        None => break "subscription ended".to_string(),
                    }
                };
                neighbors.lock().unwrap().clear();
                let known = {
                    let mut k = known_peers.lock().unwrap();
                    k.sort();
                    k.dedup();
                    k.clone()
                };
                let span = tracing::info_span!("rdm.node_admin.build.update.via-resubscribe", fabric = %fabric_name, reason = %reason, peers = known.len());
                // A refused subscribe means the gossip actor itself has stopped.
                let reopened = match gossip.subscribe(topic_id, known.clone()).await {
                    Ok(t) => t,
                    Err(e) => {
                        span.in_scope(|| tracing::info!(error = %e, "gossip has stopped; the Build topic ends"));
                        rafka_mesh_transport::membership::mark_transport_stopped(format!("gossip refused to re-open the Build topic: {e}"));
                        return;
                    }
                };
                span.in_scope(|| tracing::info!("Build topic subscription re-opened"));
                let (s, r) = reopened.split();
                *shared.write().await = s;
                receiver = r;
            }
        });
        Ok(Self { local, sender, known: known_peers_handle, joined: std::sync::Mutex::new(seed_ids_set), authority: Authority::new(node_name, seat) })
    }

    /// Join the Build topic to every live node-admin `admins` names, as the
    /// backbone does (`gossip.md` §6): each once while it stays live, again
    /// when it returns. An admin that accepted a Build while cut off reaches
    /// every admin directly when it heals, and the NeighborUp catch-up hands
    /// each one its active Builds; one neighbour alone would leave the rest to
    /// HyParView's shuffle (a minute per peer). Returns how many were joined.
    pub async fn join_admins(&self, admins: Vec<EndpointAddr>) -> usize {
        let live: Vec<iroh::EndpointId> = admins.iter().map(|a| a.id).collect();
        let fresh = rafka_mesh_transport::membership::rejoin(&self.joined, &live);
        if fresh.is_empty() {
            return 0;
        }
        self.known.lock().unwrap().extend(fresh.iter().copied());
        let sender = self.sender.read().await.clone();
        match sender.join_peers(fresh.clone()).await {
            Ok(()) => fresh.len(),
            Err(_) => 0,
        }
    }

    /// Whether this admin's Build log still decides anything: refused by name once the committed
    /// seat record names another live birth, else the holder the open record names. The
    /// fabric-primary's drive passes this before every dispatch; it holds the gate only for the
    /// check, because an attempt lasts as long as its steps and the fence is sticky.
    pub async fn fence_check(&self) -> Result<Option<String>, BuildStateError> {
        self.authority.enter_naming().await.map(|(_, holder)| holder)
    }

    /// Resolves once this admin's Build log is fenced: the drive's wait on an executor's run ends
    /// there.
    pub async fn fence_set(&self) {
        self.authority.fence_set().await
    }

    /// The lock a seat commit on this admin holds across the record's move: a decision in flight
    /// commits wholly before the record moves, and every decision after it is refused by the record.
    pub fn decisions(&self) -> Arc<tokio::sync::Mutex<()>> {
        self.authority.decisions()
    }

    /// The fabric-primary seat record moved off this birth, or this holder leaves: hand every active
    /// committed fact to the neighbours before the new holder acts. `to` is the admin the record
    /// names now (none for a leaving holder). Returns how many facts were handed on. Nothing is
    /// fenced here: the record is the fence. A send that fails is named (`via-broadcast-refused`,
    /// fact `hand-over`) and fails nothing: the new holder's catch-up and `FetchBuildFacts` carry the
    /// facts.
    pub async fn hand_over_facts(&self, by: &str, to: Option<String>) -> usize {
        let span = tracing::info_span!("rdm.node_admin.build.update.via-seat-hand-over", node = %self.authority.node, by, to = to.as_deref().unwrap_or("none"), facts = tracing::field::Empty);
        async {
            let facts = active_facts(&*self.local, None).await;
            let n = facts.len();
            let (messages, refused) = encode_chunks(facts);
            span_refused(&format!("seat hand-over by {}", self.authority.node), &refused);
            let sender = self.sender.read().await.clone();
            for bytes in messages {
                if let Err(e) = sender.broadcast_neighbors(bytes).await {
                    span_broadcast_refused(&self.authority.node, "hand-over", format!("handing the committed Build facts of {} to the neighbours ({by}): {e}", self.authority.node));
                }
            }
            tracing::Span::current().record("facts", n as u64);
            n
        }
        .instrument(span)
        .await
    }

    /// Broadcast a fabric shutdown on the control channel (fabric-mesh-lifecycle.md §11.1).
    pub(crate) async fn publish_shutdown(&self, shutdown: &crate::fabric_storage::FabricShutdown) -> Result<(), BuildStateError> {
        let sender = self.sender.read().await.clone();
        sender.broadcast(encode_shutdown(shutdown)?).await.map_err(|e| BuildStateError::Io(format!("broadcasting fabric shutdown: {e}")))
    }

    /// Broadcast `fact` (encoded as `bytes`), already committed to this admin's own log. A send
    /// that fails is named and never undoes or fails the commit: progress waits on no gossip, and
    /// the Build topic is durability and recovery evidence (node-rpc-envelope.md "Build carrier"),
    /// repaired by the neighbour-up catch-up and `FetchBuildFacts`.
    async fn publish(&self, fact: &BuildFact, bytes: bytes::Bytes) {
        let sender = self.sender.read().await.clone();
        if let Err(e) = sender.broadcast(bytes).await {
            let (kind, attempt, step) = match fact {
                BuildFact::Accepted(_) => ("accepted", None, None),
                BuildFact::Opened(o) => ("opened", Some(o.attempt), None),
                BuildFact::Claim(c) => ("claim", Some(c.attempt), None),
                BuildFact::Step(r) => ("step", Some(r.attempt), Some(r.step.as_str())),
                BuildFact::Attempt(r) => ("attempt", Some(r.attempt), None),
                BuildFact::Forget { .. } => ("forget", None, None),
            };
            tracing::info_span!(
                "rdm.node_admin.build.reject.via-broadcast-refused",
                sent_by = %self.authority.node,
                fact = kind,
                build_id = %fact.build_id(),
                attempt = attempt.map(u64::from),
                step = step,
                detail = %format!("broadcasting a Build fact committed to {}'s own log on the fabric's Build topic: {e}", self.authority.node),
            )
            .in_scope(|| tracing::info!("a committed Build fact was not broadcast: the neighbour-up catch-up and FetchBuildFacts carry it"));
        }
    }
}

#[async_trait::async_trait]
impl BuildStateAdapter for FabricBuildStateAdapter {
    async fn publish_accepted(&self, accepted: &BuildAccepted) -> Result<(), BuildStateError> {
        let _gate = self.authority.enter().await?;
        let fact = BuildFact::Accepted(accepted.clone());
        let bytes = encode(vec![fact.clone()])?;
        self.local.publish_accepted(accepted).await?;
        self.publish(&fact, bytes).await;
        Ok(())
    }

    async fn open_attempt(&self, opened: &AttemptOpened) -> Result<(), BuildStateError> {
        let _gate = self.authority.enter().await?;
        let fact = BuildFact::Opened(opened.clone());
        let bytes = encode(vec![fact.clone()])?;
        self.local.open_attempt(opened).await?;
        self.publish(&fact, bytes).await;
        Ok(())
    }

    async fn read_build(&self, build_id: &BuildId) -> Result<BuildProjection, BuildStateError> {
        self.local.read_build(build_id).await
    }

    async fn list_active(&self) -> Result<Vec<BuildProjection>, BuildStateError> {
        self.local.list_active().await
    }

    /// The fabric-primary's decision: called by `build_claim::ClaimDoor` on the seat's own log.
    async fn claim_attempt(&self, claim: &BuildAttemptClaim) -> Result<ClaimOutcome, BuildStateError> {
        let _gate = self.authority.enter().await?;
        let fact = BuildFact::Claim(claim.clone());
        let bytes = encode(vec![fact.clone()])?;
        let outcome = self.local.claim_attempt(claim).await?;
        if outcome == ClaimOutcome::Won {
            self.publish(&fact, bytes).await;
        }
        Ok(outcome)
    }

    async fn adopt_claim(&self, claim: &BuildAttemptClaim) -> Result<(), BuildStateError> {
        self.local.adopt_claim(claim).await
    }

    async fn append_step_receipt(&self, receipt: &BuildStepReceipt) -> Result<(), BuildStateError> {
        // Encoded before it commits: a receipt with no wire shape is refused by name and lands in
        // no log, never in this admin's own log alone where no other admin can read it. Once the
        // local write commits, the receipt is the step's durable commit whatever its broadcast does.
        let fact = BuildFact::Step(receipt.clone());
        let bytes = encode(vec![fact.clone()])?;
        self.local.append_step_receipt(receipt).await?;
        self.publish(&fact, bytes).await;
        Ok(())
    }

    async fn append_attempt_receipt(&self, receipt: &BuildAttemptReceipt) -> Result<(), BuildStateError> {
        let fact = BuildFact::Attempt(receipt.clone());
        let bytes = encode(vec![fact.clone()])?;
        self.local.append_attempt_receipt(receipt).await?;
        self.publish(&fact, bytes).await;
        Ok(())
    }

    async fn facts(&self) -> Result<Vec<BuildFact>, BuildStateError> {
        self.local.facts().await
    }
    async fn recovery_of(&self, path: &crate::model::PathName, node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId) -> Result<Option<(crate::build::BuildId, u32, String)>, BuildStateError> {
        self.local.recovery_of(path, node_id, incarnation).await
    }
    async fn exits_owed(&self, build_id: &crate::build::BuildId, mesh: &str) -> Result<Vec<crate::build_state::OwedExit>, BuildStateError> {
        self.local.exits_owed(build_id, mesh).await
    }

    async fn forget(&self, build_id: &BuildId) -> Result<(), BuildStateError> {
        let _gate = self.authority.enter().await?;
        let fact = BuildFact::Forget { build_id: build_id.clone() };
        let bytes = encode(vec![fact.clone()])?;
        self.local.forget(build_id).await?;
        self.publish(&fact, bytes).await;
        Ok(())
    }


    async fn publish_fabric(&self, record: &FabricRecord) -> Result<(), BuildStateError> {
        let _gate = self.authority.enter().await?;
        let sender = self.sender.read().await.clone();
        sender.broadcast(encode_fabric(record)?).await.map_err(|e| BuildStateError::Io(format!("broadcasting the Fabric record: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::build_state::BuildAttemptClaim;

    /// CONTRACT (fabric-node-lifecycle.md §6.9 Age and Wire: "the age lives in the receipt", "No frame
    /// gains a field"): a standalone stop's `Complete` receipt commits the stop's lifecycle op (the
    /// exact birth and its rafka-time), and it travels the Build topic in the shape that already
    /// carries a lifecycle op: it is encoded, decoded to the same receipt, and refused for no other
    /// operation's `Complete`, which still commits no output.
    #[test]
    fn a_stop_receipt_carries_its_birth_and_time_on_the_build_topic() {
        let op = rafka_mesh_entity::LifecycleOp {
            build_id: "b1".into(),
            attempt: 2,
            operation: "stop-node:mesh1.rpc.1".into(),
            node_id: crate::model::NodeId::mint(),
            incarnation: crate::model::IncarnationId::mint(),
            name: "mesh1.rpc.1".parse().unwrap(),
            event_at_rafka_ms: 4_242,
        };
        let receipt = |operation: &str| BuildStepReceipt {
            build_id: BuildId("b1".into()),
            attempt: 2,
            operation: operation.into(),
            step: "Complete".into(),
            outcome: crate::build_state::StepOutcome::Complete,
            output: Some(serde_json::to_value(&op).unwrap()),
            executor: Some("mesh1.admin.1".into()),
        };
        let stop = BuildFact::Step(receipt("stop-node:mesh1.rpc.1"));
        let (messages, refused) = encode_chunks(vec![stop.clone()]);
        assert!(refused.is_empty(), "{refused:?}");
        assert_eq!(BuildMessage::from_bytes(&messages[0]).unwrap().facts, vec![stop]);
        let (_, refused) = encode_chunks(vec![BuildFact::Step(receipt("create-node:mesh1.rpc.1"))]);
        assert_eq!(refused.len(), 1, "another operation's Complete commits no output");
    }

    /// Packing measures each batch once and sends those bytes: a fresh
    /// nonce of another length can never push a sent message over the limit.
    #[test]
    fn every_packed_message_fits_whatever_nonce_it_drew() {
        for round in 0..200u32 {
            let facts: Vec<BuildFact> = (0..60u32)
                .map(|i| {
                    BuildFact::Claim(BuildAttemptClaim {
                        build_id: BuildId::mint(),
                        attempt: i,
                        executor: format!("mesh{}.admin.{}", round, "x".repeat(((i * 37 + round) % 90) as usize)),
                    })
                })
                .collect();
            let (messages, refused) = encode_chunks(facts);
            assert!(refused.is_empty());
            assert!(messages.iter().all(|m| m.len() <= MAX_MESSAGE_BYTES), "every message fits");
            let sent: usize = messages.iter().map(|m| BuildMessage::from_bytes(m).unwrap().facts.len()).sum();
            assert_eq!(sent, 60, "no fact lost");
        }
    }

    /// CONTRACT (node-replace.md 2): a replace's `RenamePredecessor` receipt names the exact old
    /// birth (its output is the `LifecycleOp` of the predecessor) and travels the Build topic like
    /// every other committed output: the fabric-primary's append of it succeeds, so its live `Step`
    /// frame is made (`FramedBuilds` frames a receipt only after the append returned), and a peer
    /// folds the same receipt with the same old birth.
    #[test]
    fn a_replace_rename_receipt_naming_the_old_birth_travels_the_build_topic() {
        let op = rafka_mesh_entity::LifecycleOp {
            build_id: "bld-1".into(),
            attempt: 2,
            operation: "replace-node:mesh1.rpc.1".into(),
            node_id: rafka_mesh_entity::NodeId::mint(),
            incarnation: rafka_mesh_entity::IncarnationId::mint(),
            name: "mesh1.rpc.1".parse().unwrap(),
            event_at_rafka_ms: 7,
        };
        let receipt = BuildStepReceipt {
            build_id: BuildId("bld-1".into()),
            attempt: 2,
            operation: op.operation.clone(),
            step: crate::deployment::pipeline::ReplaceStep::RenamePredecessor.name().into(),
            outcome: crate::build_state::StepOutcome::Complete,
            output: Some(serde_json::to_value(&op).unwrap()),
            executor: Some("mesh1.admin.1".into()),
        };
        let bytes = match encode(vec![BuildFact::Step(receipt.clone())]) {
            Ok(b) => b,
            Err(e) => panic!("the rename receipt must have a wire shape: {e}"),
        };
        let back = BuildMessage::from_bytes(&bytes).unwrap().facts;
        assert_eq!(back, vec![BuildFact::Step(receipt)], "a peer folds the same receipt");
        let BuildFact::Step(s) = &back[0] else { unreachable!() };
        let named: rafka_mesh_entity::LifecycleOp = serde_json::from_value(s.output.clone().unwrap()).unwrap();
        assert_eq!((named.node_id, named.incarnation), (op.node_id, op.incarnation), "the receipt names the exact old birth");
    }

    /// CONTRACT (node-rpc-envelope.md TakeSeats: "The row is the fence: P's execution authority ends
    /// at the row's epoch, Build dispatch from the old seat is refused with Fenced, and no separate
    /// fence write precedes or follows it"): the Build log reads its authority from the committed
    /// fabric seat record. Once the record names another live birth, every decision is refused by
    /// name, naming that birth's path.name from the digest book even when the projected view does not
    /// hold it yet, and a wait on the fence resolves; no fence write is made. A record naming a birth
    /// proven gone (a vacancy the election fills before this birth's own record lands) ends nothing.
    /// What must NOT happen: a moved record that still lets the old seat decide, or a refusal naming
    /// no admin while a digest of the holder is held.
    #[tokio::test]
    async fn a_moved_fabric_seat_record_refuses_the_old_seats_decisions_with_no_fence_write() {
        use futures_util::FutureExt as _;
        use rafka_mesh_entity::{IncarnationId, NodeId, Seat, SeatHolder};
        let seats = rafka_mesh_transport::membership::SeatBook::default();
        let me = (NodeId::parse("300000000000").unwrap(), IncarnationId("p1".into()));
        let next = SeatHolder { mesh: "mesh2".into(), node_id: NodeId::parse("100000000000").unwrap(), incarnation: IncarnationId("n1".into()), epoch: 2 };
        let lost = std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashSet::<IncarnationId>::new()));
        let source = {
            let changed = Some(seats.subscribe());
            let (seats, me, lost) = (seats.clone(), me.clone(), lost.clone());
            // The digest book holds mesh2.admin.1; nothing here consults a projected view.
            let read = std::sync::Arc::new(move || crate::seat_watch::fabric_seat_record(seats.fabric().as_ref(), &me, &lost.lock().unwrap(), |id| (id.as_str() == "100000000000").then(|| "mesh2.admin.1".to_string())));
            SeatSource { read, changed }
        };
        let authority = Authority::new("mesh2.admin.2".into(), source);
        seats.take(Seat::FabricPrimary, &SeatHolder { mesh: "mesh2".into(), node_id: me.0.clone(), incarnation: me.1.clone(), epoch: 1 });
        assert!(authority.enter().await.is_ok(), "the record names this birth: it decides");
        assert!(authority.fence_set().now_or_never().is_none(), "no fence while the record names this birth");
        seats.take(Seat::FabricPrimary, &next);
        lost.lock().unwrap().insert(next.incarnation.clone());
        assert!(authority.enter().await.is_ok(), "a record naming a birth proven gone ends nothing");
        lost.lock().unwrap().clear();
        match authority.enter().await {
            Err(BuildStateError::Fenced { to, by, .. }) => {
                assert_eq!(to.as_deref(), Some("mesh2.admin.1"), "the refusal names the holder the record names");
                assert!(by.contains("100000000000") && by.contains("epoch 2"), "the reason names the record: {by}");
            }
            other => panic!("the moved record must refuse the old seat's decision by name, got {:?}", other.map(|_| ())),
        }
        assert!(authority.fence_set().now_or_never().is_some(), "a wait on the fence resolves once the record moved");
    }

    /// CONTRACT (R-A2 item 2): a seat commit on this admin waits for the decision in flight (it
    /// commits wholly before the record moves), and every decision after the move is refused by
    /// name.
    #[tokio::test]
    async fn a_seat_commit_waits_for_the_decision_in_flight_and_the_moved_record_refuses_every_later_one() {
        use rafka_mesh_entity::{IncarnationId, NodeId, Seat, SeatHolder};
        let seats = rafka_mesh_transport::membership::SeatBook::default();
        let me = (NodeId::parse("300000000000").unwrap(), IncarnationId("p1".into()));
        let source = {
            let changed = Some(seats.subscribe());
            let (seats, me) = (seats.clone(), me.clone());
            SeatSource { read: std::sync::Arc::new(move || crate::seat_watch::fabric_seat_record(seats.fabric().as_ref(), &me, &Default::default(), |_| None)), changed }
        };
        seats.take(Seat::FabricPrimary, &SeatHolder { mesh: "mesh1".into(), node_id: me.0.clone(), incarnation: me.1.clone(), epoch: 1 });
        let authority = std::sync::Arc::new(Authority::new("mesh1.admin.1".into(), source));
        let committed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let gate = authority.enter().await.unwrap();
        let (decisions, c, book) = (authority.decisions(), committed.clone(), seats.clone());
        let moving = tokio::spawn(async move {
            let _held = decisions.lock_owned().await;
            book.take(Seat::FabricPrimary, &SeatHolder { mesh: "mesh1".into(), node_id: NodeId::parse("400000000000").unwrap(), incarnation: IncarnationId("n1".into()), epoch: 2 });
            c.load(std::sync::atomic::Ordering::SeqCst)
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(!moving.is_finished(), "the seat commit waits for the decision holding the gate");
        committed.store(true, std::sync::atomic::Ordering::SeqCst);
        drop(gate);
        assert!(moving.await.unwrap(), "the record moved after the decision committed");
        let later = authority.enter().await.map(|_| ());
        match later {
            Err(BuildStateError::Fenced { node, by, to }) => {
                assert_eq!(node, "mesh1.admin.1");
                assert!(by.contains("400000000000") && by.contains("no digest of that birth is held"), "the reason names the record and why no path.name is named: {by}");
                assert_eq!(to, None);
            }
            other => panic!("a decision after the record moved must be refused by name, got {other:?}"),
        }
    }
}
