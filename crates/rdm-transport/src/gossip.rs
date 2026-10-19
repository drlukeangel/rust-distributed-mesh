//! Declared application types on the CloudEvents gossip envelope. Application ordering stays with the application.

pub use crate::envelope::Birth;
use crate::{clock::SharedClock, wire};
use bytes::Bytes;
use futures_util::StreamExt;
use iroh::EndpointAddr;
use iroh_gossip::{
    api::{Event as GossipEvent, GossipSender},
    net::Gossip,
    proto::TopicId,
};
use std::{
    collections::{BTreeSet, HashMap},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Instant,
};
use tokio::sync::{mpsc, watch, Notify};

const QUEUE: usize = 1024;
const PARTS: usize = 16;
const MAX: usize = crate::membership::MAX_FRAME;
const CHUNK: &str = crate::envelope::CHUNK_TYPE;

/// The fabric-scoped channel identity of one declared application type.
pub fn channel_id(fabric: &rdm_core::FabricId, kind: &str) -> TopicId {
    let bytes = postcard::to_allocvec(&("rdm.app", fabric, kind)).expect("strings encode");
    TopicId::from(*blake3::hash(&bytes).as_bytes())
}

/// One application event; RDM does not interpret its data or compare its sequence.
#[derive(Debug, Clone)]
pub struct Event {
    /// Application subject.
    pub subject: String,
    /// Application version, when there is one.
    pub sequence: Option<u64>,
    /// Opaque application schema name.
    pub dataschema: Option<String>,
    /// Application bytes.
    pub data: Bytes,
}

/// A whole received event, with its author's context and an application apply span.
#[derive(Debug)]
pub struct Received {
    /// Exact original author, independent of the forwarding endpoint.
    pub source: Birth,
    /// Application subject.
    pub subject: String,
    /// Application version, never compared by RDM.
    pub sequence: Option<u64>,
    /// Authored Rafka-time.
    pub time_ms: u64,
    /// Opaque schema name.
    pub dataschema: Option<String>,
    /// Complete application bytes.
    pub data: Bytes,
    /// Receive span, parented to the author's span; enter it when applying.
    pub span: tracing::Span,
}

/// A named failure to join or publish an app channel.
#[derive(Debug, thiserror::Error)]
pub enum ChannelError {
    /// This birth did not declare the type for publication or does not hold it for receipt.
    #[error("undeclared type {}", .r#type)]
    UndeclaredType {
        /// Refused type.
        r#type: String,
    },
    /// The complete event cannot fit at most sixteen complete chunk messages.
    #[error("event has {bytes} bytes; maximum {max}")]
    Oversize {
        /// Complete event size.
        bytes: usize,
        /// Maximum complete size for this metadata.
        max: usize,
    },
    /// The receiver left, or its transport ended.
    #[error("channel left")]
    Left,
}

struct State {
    sender: Mutex<Option<GossipSender>>,
    left: AtomicBool,
    stop: Notify,
    dropped: AtomicU64,
    lost: watch::Sender<u64>,
    kind: String,
    publish_declared: bool,
    author: Arc<Mutex<Option<Birth>>>,
    clock: SharedClock,
    ids: Arc<AtomicU64>,
    id_nonce: [u8; 32],
    finished: watch::Sender<bool>,
    /// The channel's gossip neighbours as its receive loop last saw them: evidence for the publish
    /// span only, never a condition on publishing.
    neighbours: AtomicUsize,
    #[cfg(feature = "testkit-gossip")]
    chunk_cut: std::sync::atomic::AtomicUsize,
}
impl State {
    fn close(&self) {
        self.left.store(true, Ordering::Release);
        self.stop.notify_one();
    }
    async fn leave(&self) {
        let mut finished = self.finished.subscribe();
        self.close();
        while !*finished.borrow_and_update() {
            if finished.changed().await.is_err() {
                break;
            }
        }
    }
    fn loss(&self) {
        self.lost.send_modify(|n| *n += 1);
    }
    async fn publish(&self, event: Event) -> Result<(), ChannelError> {
        if self.left.load(Ordering::Acquire) {
            return Err(ChannelError::Left);
        }
        if !self.publish_declared {
            tracing::info_span!("rdm.mesh.channel.reject.via-undeclared-type", event_type = %self.kind).in_scope(|| tracing::info!("this birth holds the channel but did not declare the type for publication"));
            return Err(ChannelError::UndeclaredType {
                r#type: self.kind.clone(),
            });
        }
        let source = self
            .author
            .lock()
            .unwrap()
            .clone()
            .ok_or(ChannelError::Left)?;
        let counter = self.ids.fetch_add(1, Ordering::Relaxed);
        let mut identity = blake3::Hasher::new();
        identity.update(&self.id_nonce);
        identity.update(&counter.to_le_bytes());
        let id = identity.finalize().to_hex()[..32].to_string();
        let time = self.clock.now_rafka_ms();
        let trace = rdm_core::context::CallContext {
            traceparent: crate::current_traceparent(),
            tracestate: crate::current_tracestate(),
            ..Default::default()
        }
        .sanitized()
        .0;
        let context = Attributes {
            source,
            id,
            kind: self.kind.clone(),
            subject: event.subject,
            sequence: event.sequence,
            dataschema: event.dataschema,
            time,
            trace,
        };
        let full = context
            .encode(&event.data)
            .map_err(|_| ChannelError::Left)?;
        let sender = self
            .sender
            .lock()
            .unwrap()
            .clone()
            .ok_or(ChannelError::Left)?;
        // The publication as this birth handed it to its gossip, with the neighbours its receive loop
        // last saw: an event handed over with none reaches no one (gossip is best-effort).
        let span = tracing::info_span!(
            "rdm.mesh.channel.update.via-publish",
            event_type = %self.kind,
            event_id = %context.id,
            sequence = ?context.sequence,
            bytes = full.len(),
            neighbours = self.neighbours.load(Ordering::Acquire),
            chunks = tracing::field::Empty,
            outcome = tracing::field::Empty,
        );
        let sent = tracing::Instrument::instrument(self.broadcast_whole_or_chunks(&sender, &context, full), span.clone()).await;
        span.record("outcome", if sent.is_ok() { "handed-to-gossip" } else { "refused" });
        sent
    }

    /// `full` as one gossip message, or as its chunks when it does not fit one.
    async fn broadcast_whole_or_chunks(&self, sender: &GossipSender, context: &Attributes, full: Vec<u8>) -> Result<(), ChannelError> {
        if full.len() <= MAX {
            tracing::Span::current().record("chunks", 1u64);
            return sender
                .broadcast(full.into())
                .await
                .map_err(|_| ChannelError::Left);
        }
        let mut chunk = context.chunk(15);
        let mut lo = 0;
        let mut hi = MAX;
        while lo < hi {
            let mid = (lo + hi + 1) / 2;
            let body = wire::encode(&(15_u16, PARTS as u16, vec![0_u8; mid]))
                .map_err(|_| ChannelError::Left)?;
            if chunk.encode(&body).map_err(|_| ChannelError::Left)?.len() <= MAX {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }
        if lo == 0 || full.len() > lo * PARTS {
            return Err(ChannelError::Oversize {
                bytes: full.len(),
                max: lo * PARTS,
            });
        }
        let n = full.len().div_ceil(lo);
        tracing::Span::current().record("chunks", n as u64);
        for (i, bytes) in full.chunks(lo).enumerate() {
            chunk.id = format!("{}/{i}", context.id);
            let body =
                wire::encode(&(i as u16, n as u16, bytes)).map_err(|_| ChannelError::Left)?;
            let message = chunk.encode(&body).map_err(|_| ChannelError::Left)?;
            sender
                .broadcast(message.into())
                .await
                .map_err(|_| ChannelError::Left)?;
        }
        Ok(())
    }
}

/// One owned app-channel receiver and its publisher. This value is not Clone.
pub struct Channel {
    state: Arc<State>,
    rx: Option<mpsc::Receiver<Received>>,
}
impl Channel {
    /// Publish one event using the current span's context.
    pub async fn publish(&self, event: Event) -> Result<(), ChannelError> {
        self.state.publish(event).await
    }
    /// Receive a whole event; None after leaving.
    pub async fn recv(&mut self) -> Option<Received> {
        self.rx.as_mut()?.recv().await
    }
    /// Transfer the receiver once and separate its publisher.
    pub fn split(mut self) -> (Publisher, Subscriber) {
        let rx = self.rx.take().expect("an owned channel has its receiver");
        (
            Publisher {
                state: self.state.clone(),
            },
            Subscriber {
                state: self.state.clone(),
                rx,
            },
        )
    }
    /// Events refused because this receiver's queue was full.
    pub fn dropped(&self) -> u64 {
        self.state.dropped.load(Ordering::Relaxed)
    }
    /// Local-loss generation: queue overflow or incomplete assembly eviction.
    pub fn lost(&self) -> watch::Receiver<u64> {
        self.state.lost.subscribe()
    }
    /// Leave this channel and end its receiver.
    pub async fn leave(self) {
        self.state.leave().await;
    }
}
impl Drop for Channel {
    fn drop(&mut self) {
        if self.rx.is_some() {
            self.state.close();
        }
    }
}
/// The publishing half of an owned channel.
pub struct Publisher {
    state: Arc<State>,
}
impl Publisher {
    /// Publish with the current application trace.
    pub async fn publish(&self, event: Event) -> Result<(), ChannelError> {
        self.state.publish(event).await
    }
}
/// The sole receiving half of an owned channel.
pub struct Subscriber {
    state: Arc<State>,
    rx: mpsc::Receiver<Received>,
}
impl Subscriber {
    /// Receive the next whole event.
    pub async fn recv(&mut self) -> Option<Received> {
        self.rx.recv().await
    }
    /// Read the local-loss generation.
    pub fn lost(&self) -> watch::Receiver<u64> {
        self.state.lost.subscribe()
    }
    /// Count queue overflows.
    pub fn dropped(&self) -> u64 {
        self.state.dropped.load(Ordering::Relaxed)
    }
    /// Leave the subscription.
    pub async fn leave(self) {
        self.state.leave().await;
    }
}
impl Drop for Subscriber {
    fn drop(&mut self) {
        self.state.close();
    }
}

struct Attributes {
    source: Birth,
    id: String,
    kind: String,
    subject: String,
    sequence: Option<u64>,
    dataschema: Option<String>,
    time: u64,
    trace: rdm_core::context::CallContext,
}
impl Attributes {
    fn encode(&self, data: &[u8]) -> Result<Vec<u8>, crate::wire::WireError> {
        let context = crate::envelope::Context {
            source: self.source.clone(),
            id: self.id.clone(),
            kind: self.kind.clone(),
            subject: self.subject.clone(),
            sequence: self.sequence.map(|s| format!("{s:020}")),
            time_ms: self.time,
            publication: None,
            traceparent: self.trace.traceparent.clone(),
            tracestate: self.trace.tracestate.clone(),
        };
        crate::envelope::encode_data(&context, data, self.dataschema.as_deref())
    }

    fn chunk(&self, i: usize) -> Self {
        Self {
            source: self.source.clone(),
            id: format!("{}/{i}", self.id),
            kind: CHUNK.into(),
            subject: self.id.clone(),
            sequence: None,
            dataschema: None,
            time: self.time,
            trace: self.trace.clone(),
        }
    }
}
struct Entry {
    state: Arc<State>,
    receiver: Mutex<Option<mpsc::Receiver<Received>>>,
}
struct Assembly {
    parts: Vec<Option<Vec<u8>>>,
    deadline: Instant,
}

pub(crate) struct Hub {
    gossip: Gossip,
    fabric: rdm_core::FabricId,
    peers: Vec<iroh::EndpointId>,
    joined_peers: Mutex<HashMap<String, BTreeSet<iroh::EndpointId>>>,
    own: BTreeSet<String>,
    held: Mutex<BTreeSet<String>>,
    entries: tokio::sync::Mutex<HashMap<String, Arc<Entry>>>,
    author: Arc<Mutex<Option<Birth>>>,
    clock: SharedClock,
    ids: Arc<AtomicU64>,
    id_nonce: [u8; 32],
    members: Mutex<
        HashMap<(rdm_core::NodeId, rdm_core::IncarnationId), (BTreeSet<String>, BTreeSet<String>)>,
    >,
}
impl Hub {
    pub(crate) async fn new(
        gossip: &Gossip,
        fabric: &rdm_core::FabricId,
        author: Arc<Mutex<Option<Birth>>>,
        clock: SharedClock,
        types: &[&str],
        seeds: &[EndpointAddr],
    ) -> anyhow::Result<Arc<Self>> {
        let types: Vec<String> = types.iter().map(|s| s.to_string()).collect();
        rdm_core::app_types::check(&types).map_err(|k| anyhow::anyhow!("app types: {k:?}"))?;
        let own = types.into_iter().collect();
        let me = Arc::new(Self {
            gossip: gossip.clone(),
            fabric: fabric.clone(),
            peers: seeds.iter().map(|a| a.id).collect(),
            own,
            joined_peers: Default::default(),
            held: Mutex::new(BTreeSet::new()),
            entries: Default::default(),
            author,
            clock,
            ids: Arc::new(AtomicU64::new(1)),
            id_nonce: *iroh::SecretKey::generate().public().as_bytes(),
            members: Default::default(),
        });
        me.hold(&me.own).await?;
        Ok(me)
    }
    pub(crate) async fn suspend(&self) {
        let states: Vec<_> = self
            .entries
            .lock()
            .await
            .values()
            .map(|e| e.state.clone())
            .collect();
        for state in states {
            state.leave().await;
        }
    }
    pub(crate) async fn resume(&self) -> anyhow::Result<()> {
        self.hold(&self.held()).await
    }
    pub(crate) fn types(&self) -> BTreeSet<String> {
        self.own.clone()
    }
    pub(crate) fn held(&self) -> BTreeSet<String> {
        self.held.lock().unwrap().clone()
    }
    pub(crate) async fn hold(&self, types: &BTreeSet<String>) -> anyhow::Result<()> {
        for kind in types {
            self.ensure(kind).await?;
        }
        self.held.lock().unwrap().extend(types.iter().cloned());
        Ok(())
    }
    pub(crate) fn hash(&self) -> [u8; 32] {
        rdm_core::app_types::hash(&self.held())
    }
    pub(crate) fn member(
        &self,
        node: &rdm_core::NodeId,
        incarnation: &rdm_core::IncarnationId,
    ) -> Option<(BTreeSet<String>, BTreeSet<String>)> {
        self.members
            .lock()
            .unwrap()
            .get(&(node.clone(), incarnation.clone()))
            .cloned()
    }
    pub(crate) async fn learn(
        &self,
        node: rdm_core::NodeId,
        incarnation: rdm_core::IncarnationId,
        types: BTreeSet<String>,
        held: BTreeSet<String>,
        peer: Option<iroh::EndpointId>,
    ) -> anyhow::Result<()> {
        if let Some(peer) = peer {
            let mut peers = self.joined_peers.lock().unwrap();
            for kind in &held {
                peers.entry(kind.clone()).or_default().insert(peer);
            }
        }
        self.hold(&held).await?;
        if let Some(peer) = peer {
            for kind in &held {
                let entry = self.ensure(kind).await?;
                let sender = entry.state.sender.lock().unwrap().clone();
                if let Some(sender) = sender {
                    sender.join_peers(vec![peer]).await?;
                }
            }
        }
        let mut members = self.members.lock().unwrap();
        let entry = members
            .entry((node, incarnation))
            .or_insert_with(|| (types.clone(), BTreeSet::new()));
        if entry.0 != types {
            anyhow::bail!("the exact birth already declared different app types");
        }
        entry.1.extend(held);
        Ok(())
    }
    #[cfg(feature = "testkit-gossip")]
    pub(crate) async fn drop_chunk_once(&self, kind: &str, index: usize) -> anyhow::Result<()> {
        anyhow::ensure!(index < PARTS, "chunk index exceeds the sixteen-part limit");
        anyhow::ensure!(
            self.held.lock().unwrap().contains(kind),
            "the type is not held"
        );
        let entry = self.ensure(kind).await?;
        entry.state.chunk_cut.store(index, Ordering::Release);
        Ok(())
    }
    pub(crate) async fn join(&self, kind: &str) -> Result<Channel, ChannelError> {
        if !self.held.lock().unwrap().contains(kind) {
            tracing::info_span!(
                "rdm.mesh.channel.reject.via-undeclared-type",
                event_type = kind
            )
            .in_scope(|| tracing::info!("the birth has not declared or learned this type"));
            return Err(ChannelError::UndeclaredType {
                r#type: kind.into(),
            });
        }
        let e = self.ensure(kind).await.map_err(|_| ChannelError::Left)?;
        let rx = e
            .receiver
            .lock()
            .unwrap()
            .take()
            .ok_or(ChannelError::Left)?;
        Ok(Channel {
            state: e.state.clone(),
            rx: Some(rx),
        })
    }
    async fn ensure(&self, kind: &str) -> anyhow::Result<Arc<Entry>> {
        let mut entries = self.entries.lock().await;
        if let Some(e) = entries
            .get(kind)
            .filter(|e| !e.state.left.load(Ordering::Acquire))
        {
            return Ok(e.clone());
        }
        let mut peers = self.peers.clone();
        if let Some(learned) = self.joined_peers.lock().unwrap().get(kind) {
            peers.extend(learned);
        }
        peers.sort_unstable();
        peers.dedup();
        let channel = self
            .gossip
            .subscribe(channel_id(&self.fabric, kind), peers)
            .await?;
        let (sender, mut stream) = channel.split();
        let (tx, rx) = mpsc::channel(QUEUE);
        let state = Arc::new(State {
            sender: Mutex::new(Some(sender)),
            left: AtomicBool::new(false),
            stop: Notify::new(),
            dropped: AtomicU64::new(0),
            lost: watch::channel(0).0,
            kind: kind.into(),
            publish_declared: self.own.contains(kind),
            author: self.author.clone(),
            clock: self.clock.clone(),
            ids: self.ids.clone(),
            id_nonce: self.id_nonce,
            finished: watch::channel(false).0,
            neighbours: AtomicUsize::new(0),
            #[cfg(feature = "testkit-gossip")]
            chunk_cut: std::sync::atomic::AtomicUsize::new(usize::MAX),
        });
        let e = Arc::new(Entry {
            state: state.clone(),
            receiver: Mutex::new(Some(rx)),
        });
        entries.insert(kind.into(), e.clone());
        let fabric = self.fabric.clone();
        tokio::spawn(async move {
            let mut assemblies: HashMap<(Birth, String), Assembly> = HashMap::new();
            // Use the substrate's message-id retention, rather than retaining every app write
            // for the receiver's lifetime. Only complete event identities enter this hold.
            let retention = iroh_gossip::proto::PlumtreeConfig::default().message_id_retention;
            let mut seen = iroh_gossip::proto::util::TimeBoundCache::default();
            loop {
                let next = assemblies.values().map(|a| a.deadline).min();
                let expiry = async {
                    match next {
                        Some(at) => tokio::time::sleep_until(at.into()).await,
                        None => std::future::pending().await,
                    }
                };
                let item = tokio::select! {
                    _ = state.stop.notified() => break,
                    _ = expiry => {
                        let now = Instant::now();
                        assemblies.retain(|(source, id), a| {
                            if a.deadline > now { return true; }
                            tracing::info_span!("rdm.mesh.membership.reject.via-chunk-incomplete", source = %source.uri(), event_id = id, event_type = %state.kind).in_scope(|| tracing::info!("unfinished event evicted after the staleness floor"));
                            state.loss(); false
                        });
                        continue;
                    },
                    item = stream.next() => item,
                };
                seen.expire_until(Instant::now().into());
                if let Some(Ok(GossipEvent::NeighborUp(peer))) = &item {
                    let neighbours = state.neighbours.fetch_add(1, Ordering::AcqRel) + 1;
                    tracing::info_span!("rdm.mesh.channel.update.via-neighbour", event_type = %state.kind, peer = %peer, neighbours).in_scope(|| tracing::info!("application channel neighbour joined"));
                    continue;
                }
                if let Some(Ok(GossipEvent::NeighborDown(peer))) = &item {
                    let neighbours = state.neighbours.fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| Some(n.saturating_sub(1))).map_or(0, |n| n.saturating_sub(1));
                    tracing::info_span!("rdm.mesh.channel.update.via-neighbour-down", event_type = %state.kind, peer = %peer, neighbours).in_scope(|| tracing::info!("application channel neighbour left"));
                    continue;
                }
                let Some(Ok(GossipEvent::Received(message))) = item else {
                    if item.is_none() {
                        break;
                    }
                    continue;
                };
                let mut trace = Default::default();
                let mut dropped = Vec::new();
                let header =
                    crate::envelope::decode_header(&message.content, &mut trace, &mut dropped, MAX);
                if let Err(error) = &header {
                    tracing::info_span!("rdm.mesh.membership.reject.via-undecodable-frame", event_type = %state.kind, bytes = message.content.len(), error = ?error).in_scope(|| tracing::info!("application envelope refused"));
                }
                if let Ok(mut h) = header {
                    if h.source.fabric != fabric {
                        tracing::info_span!("rdm.mesh.membership.reject.via-undecodable-frame", event_type = %state.kind, error = "source fabric differs from the channel").in_scope(|| tracing::info!("application envelope refused"));
                        continue;
                    }
                    if h.kind == CHUNK {
                        if h.sequence.is_some() || h.dataschema.is_some() {
                            continue;
                        }
                        let Ok((i, n, bytes)) = wire::decode::<(u16, u16, Vec<u8>)>(&h.data) else {
                            continue;
                        };
                        if n == 0
                            || n as usize > PARTS
                            || i >= n
                            || h.id != format!("{}/{i}", h.subject)
                        {
                            continue;
                        }
                        #[cfg(feature = "testkit-gossip")]
                        if state
                            .chunk_cut
                            .compare_exchange(
                                i as usize,
                                usize::MAX,
                                Ordering::AcqRel,
                                Ordering::Acquire,
                            )
                            .is_ok()
                        {
                            tracing::info_span!("rdm.testkit.channel.reject.via-chunk-cut", event_type = %state.kind, event_id = %h.subject, chunk_index = i).in_scope(|| tracing::info!("the armed testkit cut drops this one chunk"));
                            continue;
                        }
                        let key = (h.source.clone(), h.subject.clone());
                        if seen.contains_key(&event_identity(&key.0, &key.1)) {
                            continue;
                        }
                        let a = assemblies.entry(key.clone()).or_insert_with(|| Assembly {
                            parts: vec![None; n as usize],
                            deadline: Instant::now() + rdm_core::cadence::staleness_floor(),
                        });
                        if a.parts.len() != n as usize
                            || a.parts[i as usize]
                                .as_ref()
                                .is_some_and(|old| *old != bytes)
                        {
                            continue;
                        }
                        a.parts[i as usize] = Some(bytes);
                        if a.parts.iter().any(Option::is_none) {
                            continue;
                        }
                        let a = assemblies.remove(&key).unwrap();
                        let full: Vec<u8> = a.parts.into_iter().flatten().flatten().collect();
                        let Ok(whole) = crate::envelope::decode_header(
                            &full,
                            &mut trace,
                            &mut dropped,
                            MAX * PARTS,
                        ) else {
                            continue;
                        };
                        if whole.source != key.0 || whole.id != key.1 {
                            continue;
                        }
                        h = whole;
                    }
                    if h.kind != state.kind {
                        tracing::info_span!("rdm.mesh.channel.reject.via-undeclared-type", event_type = %h.kind, channel_type = %state.kind).in_scope(|| tracing::info!("type does not belong to this declared channel"));
                        continue;
                    }
                    let sequence = match &h.sequence {
                        None => None,
                        Some(s) if s.len() == 20 && s.bytes().all(|b| b.is_ascii_digit()) => {
                            match s.parse() {
                                Ok(v) => Some(v),
                                Err(_) => continue,
                            }
                        }
                        _ => continue,
                    };
                    let identity = event_identity(&h.source, &h.id);
                    if seen.contains_key(&identity) {
                        continue;
                    }
                    seen.insert(identity, (), (Instant::now() + retention).into());
                    let span = tracing::info_span!("rdm.mesh.membership.update.via-frame", r#type = %h.kind, subject = %h.subject, sequence = ?sequence, source = %h.source.uri(), bytes = h.data.len());
                    if let Some(parent) = trace.traceparent.as_deref() {
                        crate::set_remote_parent(&span, parent, trace.tracestate.as_deref());
                    }
                    if !dropped.is_empty() {
                        span.in_scope(|| tracing::info_span!("rdm.mesh.membership.update.via-context-dropped", context_dropped = %rdm_core::context::dropped_as_str(&dropped)).in_scope(|| tracing::info!("malformed tracing metadata dropped; the event continues")));
                    }
                    deliver(
                        &state,
                        &tx,
                        Received {
                            source: h.source,
                            subject: h.subject,
                            sequence,
                            time_ms: h.time_ms,
                            dataschema: h.dataschema,
                            data: h.data.into(),
                            span,
                        },
                    );
                }
            }
            state.left.store(true, Ordering::Release);
            state.sender.lock().unwrap().take();
            drop(stream);
            state.finished.send_replace(true);
        });
        Ok(e)
    }
}
fn event_identity(source: &Birth, id: &str) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(source.uri().as_bytes());
    hash.update(&[0]);
    hash.update(id.as_bytes());
    *hash.finalize().as_bytes()
}
fn deliver(state: &State, tx: &mpsc::Sender<Received>, event: Received) {
    if let Err(mpsc::error::TrySendError::Full(event)) = tx.try_send(event) {
        state.dropped.fetch_add(1, Ordering::Relaxed);
        state.loss();
        event.span.in_scope(|| tracing::info_span!("rdm.mesh.channel.reject.via-queue-full", event_type = %state.kind, dropped = state.dropped.load(Ordering::Relaxed)).in_scope(|| tracing::info!("the newest event is dropped; receiving continues")));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    /// A hand encoder writes postcard lengths and literal metadata independently of the codec.
    #[test]
    fn application_and_chunk_envelopes_match_hand_goldens_and_the_complete_size_ceiling() {
        fn varint(mut n: usize, out: &mut Vec<u8>) {
            loop {
                let b = (n & 127) as u8;
                n >>= 7;
                out.push(b | if n == 0 { 0 } else { 128 });
                if n == 0 {
                    break;
                }
            }
        }
        fn text(s: &str, out: &mut Vec<u8>) {
            varint(s.len(), out);
            out.extend(s.as_bytes());
        }
        fn expected(pairs: &[(&str, &str)], data: &[u8]) -> Vec<u8> {
            let mut out = Vec::new();
            varint(pairs.len(), &mut out);
            for (n, v) in pairs {
                text(n, &mut out);
                text(v, &mut out);
            }
            varint(data.len(), &mut out);
            out.extend(data);
            out
        }
        let source = Birth {
            fabric: rdm_core::FabricId::parse("1123456789ab").unwrap(),
            node: rdm_core::NodeId::parse("0123456789ab").unwrap(),
            incarnation: rdm_core::IncarnationId::parse("yrz49j303yh4").unwrap(),
        };
        let event = Attributes {
            source,
            id: "0123456789abcdef0123456789abcdef".into(),
            kind: "app.test".into(),
            subject: "row/1".into(),
            sequence: Some(7),
            dataschema: Some("app.v1".into()),
            time: 1,
            trace: Default::default(),
        };
        let pairs = [
            ("specversion", "1.0"),
            ("id", "0123456789abcdef0123456789abcdef"),
            ("source", "rdm://1123456789ab/0123456789ab/yrz49j303yh4"),
            ("type", "app.test"),
            ("subject", "row/1"),
            ("sequence", "00000000000000000007"),
            ("time", "1970-01-01T00:00:00.001Z"),
            ("datacontenttype", "application/octet-stream"),
            ("dataschema", "app.v1"),
        ];
        let golden = expected(&pairs, &[0, 1, 255]);
        assert_eq!(event.encode(&[0, 1, 255]).unwrap(), golden);
        let decoded =
            crate::envelope::decode_header(&golden, &mut Default::default(), &mut Vec::new(), MAX)
                .unwrap();
        assert_eq!(decoded.data, vec![0, 1, 255]);
        assert_eq!(decoded.kind, "app.test");
        let chunk = event.chunk(15);
        // Positional chunk body: i=15, n=16, byte vector length=3690 (ea 1c).
        let mut body = vec![15, 16, 0xea, 0x1c];
        body.extend(vec![0x5a; 3690]);
        let pairs = [
            ("specversion", "1.0"),
            ("id", "0123456789abcdef0123456789abcdef/15"),
            ("source", "rdm://1123456789ab/0123456789ab/yrz49j303yh4"),
            ("type", "rdm.gossip.chunk"),
            ("subject", "0123456789abcdef0123456789abcdef"),
            ("time", "1970-01-01T00:00:00.001Z"),
            ("datacontenttype", "application/octet-stream"),
        ];
        let golden = expected(&pairs, &body);
        assert_eq!(chunk.encode(&body).unwrap(), golden);
        assert!(golden.len() <= MAX);
        assert!(golden.len() > 3900);
        println!("complete chunk golden: {} / {} bytes", golden.len(), MAX);
    }

    /// Exercise the actual receive queue, without depending on best-effort network throughput.
    #[test]
    fn a_full_channel_queue_drops_the_newest_by_name_and_the_carrier_keeps_receiving() {
        let dir = std::env::temp_dir().join(format!("channel-queue-{}", rdm_core::NodeId::mint()));
        let capture = crate::EvidenceCapture::start(&dir, "channel-queue").unwrap();
        capture.run(1, async {
            let state = State {
                sender: Mutex::new(None),
                left: AtomicBool::new(false),
                stop: Notify::new(),
                dropped: AtomicU64::new(0),
                lost: watch::channel(0).0,
                kind: "app.queue".into(),
                publish_declared: true,
                author: Default::default(),
                clock: crate::clock::os_clock(),
                ids: Arc::new(AtomicU64::new(1)),
                id_nonce: [0; 32],
                finished: watch::channel(false).0,
                neighbours: AtomicUsize::new(0),
                #[cfg(feature = "testkit-gossip")]
                chunk_cut: std::sync::atomic::AtomicUsize::new(usize::MAX),
            };
            let (tx, mut rx) = mpsc::channel(QUEUE);
            let birth = Birth {
                fabric: rdm_core::FabricId::mint(),
                node: rdm_core::NodeId::mint(),
                incarnation: rdm_core::IncarnationId::mint(),
            };
            let event = |n: usize| Received {
                source: birth.clone(),
                subject: n.to_string(),
                sequence: Some(n as u64),
                dataschema: None,
                time_ms: 1,
                data: Bytes::new(),
                span: tracing::info_span!("rdm.mesh.membership.update.via-frame", subject = n),
            };
            for n in 0..QUEUE {
                deliver(&state, &tx, event(n));
            }
            let generation = *state.lost.borrow();
            deliver(&state, &tx, event(QUEUE));
            assert_eq!(state.dropped.load(Ordering::Relaxed), 1);
            assert_eq!(*state.lost.borrow(), generation + 1);
            for n in 0..QUEUE {
                assert_eq!(rx.recv().await.unwrap().subject, n.to_string());
            }
            deliver(&state, &tx, event(QUEUE + 1));
            assert_eq!(rx.recv().await.unwrap().subject, (QUEUE + 1).to_string());
            assert!(capture
                .spans()
                .unwrap()
                .iter()
                .any(|s| s["name"] == "rdm.mesh.channel.reject.via-queue-full"));
        });
        let _ = std::fs::remove_dir_all(dir);
    }
}
