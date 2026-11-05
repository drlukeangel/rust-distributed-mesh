//! Application channels on the CloudEvents gossip envelope: RDM provides the channels, an application's
//! nodes join the channels its own code names (gossip.md §8, Q362). Application ordering stays with the application.

pub use crate::envelope::Birth;
use rdm_transport::{clock::SharedClock, wire};
use bytes::Bytes;
use futures_util::StreamExt;
use iroh_gossip::api::{Event as GossipEvent, GossipSender};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Instant,
};
use tokio::sync::{mpsc, watch, Notify};

pub(crate) const QUEUE: usize = 1024;
const PARTS: usize = 16;
const MAX: usize = crate::frame::MAX_FRAME;
const CHUNK: &str = crate::envelope::CHUNK_TYPE;

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
    /// This birth's author identity is not set yet: nothing can name the event's source.
    #[error("no author set for this birth")]
    NoAuthor,
    /// The event or one of its chunks could not be encoded.
    #[error("event encode failed: {0}")]
    Encode(String),
    /// Gossip refused the event, or its chunk `chunk` of `of` (a chunked event part-way through).
    #[error("broadcast of chunk {chunk} of {of} failed: {detail}")]
    Broadcast {
        /// The chunk gossip refused (0 for a whole event).
        chunk: usize,
        /// The event's chunk count (1 for a whole event).
        of: usize,
        /// Gossip's error.
        detail: String,
    },
    /// The channel could not be subscribed.
    #[error("subscribe to {} failed: {detail}", .r#type)]
    Subscribe {
        /// The type whose channel was refused.
        r#type: String,
        /// Gossip's error.
        detail: String,
    },
    /// The type's one receiver is already held by an earlier join.
    #[error("the receiver of {} is already held", .r#type)]
    ReceiverHeld {
        /// The type joined again.
        r#type: String,
    },
}

pub(crate) struct State {
    sender: Mutex<Option<GossipSender>>,
    left: AtomicBool,
    stop: Notify,
    dropped: AtomicU64,
    lost: watch::Sender<u64>,
    kind: String,
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
    /// The delivery state of the application channel `kind`, sending through `sender`.
    pub(crate) fn new(kind: &str, sender: GossipSender, author: Arc<Mutex<Option<Birth>>>, clock: SharedClock, ids: Arc<AtomicU64>, id_nonce: [u8; 32]) -> Arc<Self> {
        Arc::new(Self {
            sender: Mutex::new(Some(sender)),
            left: AtomicBool::new(false),
            stop: Notify::new(),
            dropped: AtomicU64::new(0),
            lost: watch::channel(0).0,
            kind: kind.into(),
            author,
            clock,
            ids,
            id_nonce,
            finished: watch::channel(false).0,
            neighbours: AtomicUsize::new(0),
            #[cfg(feature = "testkit-gossip")]
            chunk_cut: std::sync::atomic::AtomicUsize::new(usize::MAX),
        })
    }

    /// Whether the channel was left (its receive loop ended or it was stopped).
    pub(crate) fn is_left(&self) -> bool {
        self.left.load(Ordering::Acquire)
    }

    /// The channel's sender while it is joined.
    pub(crate) fn sender(&self) -> Option<GossipSender> {
        self.sender.lock().unwrap().clone()
    }

    /// The testkit cut: drop the chunk at `index` of the next multi-chunk event once.
    #[cfg(feature = "testkit-gossip")]
    pub(crate) fn cut_chunk_once(&self, index: usize) -> anyhow::Result<()> {
        anyhow::ensure!(index < PARTS, "chunk index exceeds the sixteen-part limit");
        self.chunk_cut.store(index, Ordering::Release);
        Ok(())
    }
    fn close(&self) {
        self.left.store(true, Ordering::Release);
        self.stop.notify_one();
    }
    pub(crate) async fn leave(&self) {
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
        let source = self
            .author
            .lock()
            .unwrap()
            .clone()
            .ok_or(ChannelError::NoAuthor)?;
        let counter = self.ids.fetch_add(1, Ordering::Relaxed);
        let mut identity = blake3::Hasher::new();
        identity.update(&self.id_nonce);
        identity.update(&counter.to_le_bytes());
        let id = identity.finalize().to_hex()[..32].to_string();
        let time = self.clock.now_rafka_ms();
        let trace = rdm_core::context::CallContext {
            traceparent: rdm_transport::current_traceparent(),
            tracestate: rdm_transport::current_tracestate(),
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
            .map_err(|e| ChannelError::Encode(e.to_string()))?;
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
                .map_err(|e| ChannelError::Broadcast { chunk: 0, of: 1, detail: e.to_string() });
        }
        let mut chunk = context.chunk(15);
        let mut lo = 0;
        let mut hi = MAX;
        while lo < hi {
            let mid = (lo + hi + 1) / 2;
            let body = wire::encode(&(15_u16, PARTS as u16, vec![0_u8; mid]))
                .map_err(|e| ChannelError::Encode(e.to_string()))?;
            if chunk.encode(&body).map_err(|e| ChannelError::Encode(e.to_string()))?.len() <= MAX {
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
                wire::encode(&(i as u16, n as u16, bytes)).map_err(|e| ChannelError::Encode(e.to_string()))?;
            let message = chunk.encode(&body).map_err(|e| ChannelError::Encode(e.to_string()))?;
            sender
                .broadcast(message.into())
                .await
                .map_err(|e| ChannelError::Broadcast { chunk: i, of: n, detail: e.to_string() })?;
        }
        Ok(())
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
    fn encode(&self, data: &[u8]) -> Result<Vec<u8>, rdm_transport::wire::WireError> {
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
struct Assembly {
    parts: Vec<Option<Vec<u8>>>,
    deadline: Instant,
}

/// The publisher and the one receiver of an application channel joined through `Channels::join`,
/// handed once (`Channels::app`).
pub(crate) fn pair(state: Arc<State>, rx: mpsc::Receiver<Received>) -> (Publisher, Subscriber) {
    (Publisher { state: state.clone() }, Subscriber { state, rx })
}

/// The application channel's delivery (gossip.md §8): each whole event received on `stream` into
/// `tx`, its chunks assembled and an unfinished one evicted after the staleness floor; the channel
/// left when `stream` ends.
pub(crate) fn deliver_from(state: Arc<State>, mut stream: iroh_gossip::api::GossipReceiver, tx: mpsc::Sender<Received>, fabric: rdm_core::FabricId) {
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
                        tracing::info_span!("rdm.mesh.topology.reject.via-chunk-incomplete", source = %source.uri(), event_id = id, event_type = %state.kind).in_scope(|| tracing::info!("unfinished event evicted after the staleness floor"));
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
                tracing::info_span!("rdm.mesh.topology.reject.via-undecodable-frame", event_type = %state.kind, bytes = message.content.len(), error = ?error).in_scope(|| tracing::info!("application envelope refused"));
            }
            if let Ok(mut h) = header {
                if h.source.fabric != fabric {
                    tracing::info_span!("rdm.mesh.topology.reject.via-undecodable-frame", event_type = %state.kind, error = "source fabric differs from the channel").in_scope(|| tracing::info!("application envelope refused"));
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
                let sequence = match &h.sequence {
                    None => None,
                    Some(s) => match Some(s).filter(|s| s.len() == 20 && s.bytes().all(|b| b.is_ascii_digit())).and_then(|s| s.parse().ok()) {
                        Some(v) => Some(v),
                        None => {
                            tracing::info_span!("rdm.mesh.channel.reject.via-malformed-sequence", event_type = %h.kind, event_id = %h.id, sequence_len = s.len()).in_scope(|| tracing::info!("an event's sequence is 20 decimal digits: the frame is dropped"));
                            continue;
                        }
                    },
                };
                let identity = event_identity(&h.source, &h.id);
                if seen.contains_key(&identity) {
                    continue;
                }
                seen.insert(identity, (), (Instant::now() + retention).into());
                // `delivered_from`: the neighbour that handed this node the event, its carrier (iroh-gossip's
                // `delivered_from`); `source` is its author.
                let span = tracing::info_span!("rdm.mesh.topology.update.via-frame", r#type = %h.kind, subject = %h.subject, sequence = ?sequence, source = %h.source.uri(), delivered_from = %message.delivered_from, bytes = h.data.len());
                if let Some(parent) = trace.traceparent.as_deref() {
                    rdm_transport::set_remote_parent(&span, parent, trace.tracestate.as_deref());
                }
                if !dropped.is_empty() {
                    span.in_scope(|| tracing::info_span!("rdm.mesh.topology.update.via-context-dropped", context_dropped = %rdm_core::context::dropped_as_str(&dropped)).in_scope(|| tracing::info!("malformed tracing metadata dropped; the event continues")));
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

    /// CONTRACT (one error arm per fact): a publish before this birth's author identity is set is
    /// refused `NoAuthor`, never `Left`: the channel has not ended, its event has no source yet.
    #[tokio::test]
    async fn a_publish_with_no_author_set_is_refused_no_author_not_left() {
        let state = State {
            sender: Mutex::new(None),
            left: AtomicBool::new(false),
            stop: Notify::new(),
            dropped: AtomicU64::new(0),
            lost: watch::channel(0).0,
            kind: "app.author".into(),
            author: Default::default(),
            clock: rdm_transport::clock::os_clock(),
            ids: Arc::new(AtomicU64::new(1)),
            id_nonce: [0; 32],
            finished: watch::channel(false).0,
            neighbours: AtomicUsize::new(0),
            #[cfg(feature = "testkit-gossip")]
            chunk_cut: std::sync::atomic::AtomicUsize::new(usize::MAX),
        };
        let event = Event { subject: "s".into(), sequence: None, dataschema: None, data: Bytes::from_static(b"x") };
        assert!(matches!(state.publish(event).await, Err(ChannelError::NoAuthor)), "no author: refused NoAuthor");
        state.close();
        let event = Event { subject: "s".into(), sequence: None, dataschema: None, data: Bytes::from_static(b"x") };
        assert!(matches!(state.publish(event).await, Err(ChannelError::Left)), "a left channel: Left");
    }

    /// Exercise the actual receive queue, without depending on best-effort network throughput.
    #[test]
    fn a_full_channel_queue_drops_the_newest_by_name_and_the_carrier_keeps_receiving() {
        let dir = std::env::temp_dir().join(format!("channel-queue-{}", rdm_core::NodeId::mint()));
        let capture = rdm_transport::EvidenceCapture::start(&dir, "channel-queue").unwrap();
        capture.run(1, async {
            let state = State {
                sender: Mutex::new(None),
                left: AtomicBool::new(false),
                stop: Notify::new(),
                dropped: AtomicU64::new(0),
                lost: watch::channel(0).0,
                kind: "app.queue".into(),
                    author: Default::default(),
                clock: rdm_transport::clock::os_clock(),
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
                span: tracing::info_span!("rdm.mesh.topology.update.via-frame", subject = n),
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
