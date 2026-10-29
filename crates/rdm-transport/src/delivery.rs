//! The delivery layer (node-rpc.md §43, §33; node-rpc-envelope.md "Transport frames").
//!
//! One [`Delivery`] per process, shared by its client and its server: the birth that mints its
//! frames' identities, the one `delivery_seq` counter, the duplicate windows of every sender, the
//! caller's open invocations keyed by `invocation_id`, the responder's owed response frames, and
//! the live connections a resend or a resent response rides. Nothing here is durable: an ack proves
//! receipt, never execution, and recovery is the operation owner's.
//!
//! - A request is owed until acked: the client resends it with the same identity on the schedule
//!   (the publish cadence, doubling with jitter, capped at the staleness floor), one attempt in
//!   flight, over the ordinary dial.
//! - The receiver suppresses a repeated identity: pending and unacked while the original has not
//!   reached its acceptance boundary, acked again without dispatch after it.
//! - A response frame is owed too: written on the request's stream, and when that stream is gone,
//!   resent on a new stream to whichever endpoint last delivered the request, until the caller acks.
//! - Response and `Ack` frames reach this demultiplexer before dispatch and before lifecycle
//!   admission.

use crate::resolve::{NodeResolver, NodeTarget};
use iroh::endpoint::{Connection, ReadError, RecvStream, SendStream, VarInt};
use rdm_core::codes::ResetCode;
use rdm_core::delivery::{resend_after, Admitted, Arrival, RecvWindow, ResponseOrder, SendWindow};
use rdm_core::framing::{decode_header, decode_varint, encode_ack_frame, encode_response_frame, Birth, DeliveryId, HeaderRead, TransportHeader, Varint};
use rdm_core::{IncarnationId, NodeId};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch};

/// The largest reply body read for a response frame whose invocation this process does not hold
/// open: it is read past (and acked when its invocation completed), never delivered.
const MAX_UNHELD_REPLY_BYTES: usize = 16 * 1024 * 1024;

/// The resend schedule's two cadences.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Pacing {
    /// The first resend's delay: the publish cadence.
    pub cadence: Duration,
    /// The cap: the staleness floor.
    pub floor: Duration,
    /// Whether each delay carries its ±25 % jitter.
    pub jitter: bool,
}

impl Pacing {
    /// The process's own cadences (`rdm_core::cadence`): the gossip interval, the staleness floor.
    pub fn from_cadences() -> Self {
        Self { cadence: rdm_core::cadence::gossip_interval(), floor: rdm_core::cadence::staleness_floor(), jitter: true }
    }

    /// The delay before resend `attempt` (1 is the first).
    pub fn after(&self, attempt: u32) -> Duration {
        let jitter = if self.jitter { rand_jitter() } else { 0.0 };
        resend_after(attempt, self.cadence, self.floor, jitter)
    }
}

fn rand_jitter() -> f64 {
    // A uniform draw in [-0.25, 0.25] from the clock's low bits: spreading resends needs no CSPRNG.
    let n = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.subsec_nanos()).unwrap_or(0);
    (f64::from(n % 10_001) / 10_000.0 - 0.5) / 2.0
}

/// Counters a cell reads (no root span per frame: these are the hot path).
#[derive(Debug, Default)]
pub struct DeliveryStats {
    /// Request frames seen for the first time.
    pub requests_new: AtomicU64,
    /// Repeated requests held pending while the original had not reached its acceptance boundary.
    pub duplicates_pending: AtomicU64,
    /// Repeated requests acked again, without dispatch.
    pub duplicates_acked: AtomicU64,
    /// `Ack` frames written.
    pub acks_sent: AtomicU64,
    /// `Ack` frames received that settled something.
    pub acks_received: AtomicU64,
    /// Request resends.
    pub resends: AtomicU64,
    /// Response frames written again on a new stream.
    pub responses_resent: AtomicU64,
    /// Response frames for a completed invocation, acked and dropped.
    pub responses_of_completed: AtomicU64,
    /// Response frames for an invocation this process never held: dropped, not acked.
    pub responses_unheld: AtomicU64,
    /// Streams reset `424` by this layer.
    pub violations: AtomicU64,
}

impl DeliveryStats {
    /// Read `c`.
    pub fn get(c: &AtomicU64) -> u64 {
        c.load(Ordering::SeqCst)
    }
}

/// Test seams. Unarmed, each is inert; the frames and the wire are the production ones.
#[derive(Debug, Default)]
pub struct DeliverySeams {
    drop_next_request_ack: AtomicBool,
    drop_next_response_ack: AtomicBool,
    cut_after_next_request: AtomicBool,
    hold_next_request_ack: AtomicBool,
    request_ack_released: tokio::sync::Notify,
    request_ack_written: tokio::sync::Notify,
}

impl DeliverySeams {
    /// The responder writes no `Ack` for the next request it accepts (a lost ack).
    pub fn drop_next_request_ack(&self) {
        self.drop_next_request_ack.store(true, Ordering::SeqCst);
    }

    /// The caller writes no `Ack` for the next response frame it delivers, and the stream it arrived
    /// on dies before one could be written (a lost ack: on a live stream an ack is never lost).
    pub fn drop_next_response_ack(&self) {
        self.drop_next_response_ack.store(true, Ordering::SeqCst);
    }

    /// The responder closes the connection the next request frame arrives on, once the frame is
    /// read and before it is dispatched or acknowledged.
    pub fn cut_connection_after_next_request(&self) {
        self.cut_after_next_request.store(true, Ordering::SeqCst);
    }

    /// The responder holds the `Ack` of the next request it accepts until
    /// [`release_request_ack`](Self::release_request_ack): the ack's write moves after whatever the
    /// cell does in between (a connection cut).
    pub fn hold_next_request_ack(&self) {
        self.hold_next_request_ack.store(true, Ordering::SeqCst);
    }

    /// Let the held request `Ack` be written; resolves once its write has ended, written or failed.
    pub async fn release_request_ack(&self) {
        self.request_ack_released.notify_one();
        self.request_ack_written.notified().await;
    }

    fn take(flag: &AtomicBool) -> bool {
        flag.swap(false, Ordering::SeqCst)
    }

    /// Whether the connection cut is armed for this request (and disarm it).
    pub(crate) fn take_cut(&self) -> bool {
        Self::take(&self.cut_after_next_request)
    }
}

pub(crate) type BoxFut<T> = Pin<Box<dyn Future<Output = T> + Send>>;
type Dialer = Arc<dyn Fn(iroh::PublicKey) -> BoxFut<Option<Connection>> + Send + Sync>;

/// What a repeated request is answered with, once its original settles.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Settled {
    /// Accepted (or answered): the repeat is acked, then its stream ends.
    Accepted,
    /// The original ended in a reset: the repeat is reset with the same code.
    Ended(u64),
    /// The original was never dispatched (its frame never completed): the repeat is stopped and
    /// the sender's next resend is a first arrival.
    Dropped,
}

enum Seen {
    Pending(Vec<oneshot::Sender<Settled>>),
    Accepted,
    Ended(u64),
}

/// An event on a caller's open invocation.
pub(crate) enum SlotEvent {
    /// A response frame, in order: its reply bytes and, for a stream's frame, the ack its owner
    /// writes once it can recover the frame.
    Frame(Vec<u8>, Option<AckOnRecovery>),
    /// The invocation's response broke its order: `424`.
    Violation(String),
}

/// Where an ack goes: the stream the frame arrived on, or a new stream of its connection.
#[derive(Clone)]
pub(crate) enum AckTo {
    Stream(FrameOut),
    NewStream(Connection),
}

/// A stream frame's ack, written once its owner can recover the frame (node-rpc.md §31, §43): never
/// on the read alone. Until then the frame is owed: a copy the responder resends after a reconnect
/// is not handed to the owner again, and moves the ack to the stream that copy arrived on.
pub(crate) struct AckOnRecovery {
    d: Delivery,
    invocation: DeliveryId,
    seq: u32,
    delivery_seq: u64,
}

impl AckOnRecovery {
    pub(crate) fn position(&self) -> (&DeliveryId, u32) {
        (&self.invocation, self.seq)
    }

    /// The owner can recover the frame: ack it on the stream its latest copy arrived on.
    pub(crate) async fn write(self) {
        let held = self.d.inner.st.lock().unwrap().calls.get_mut(&self.invocation).and_then(|s| s.unrecovered.remove(&self.delivery_seq));
        let Some((ack, to)) = held else {
            tracing::info_span!("rdm.node_rpc.delivery.reject.via-ack-without-invocation", delivery_id = %self.invocation, seq = self.seq, frame_delivery_seq = self.delivery_seq)
                .in_scope(|| tracing::warn!("the invocation holds no owed ack for this frame: no ack is written"));
            return;
        };
        match &to {
            AckTo::Stream(out) => {
                self.d.write_ack(out, &ack, AckOf::Response).await;
            }
            AckTo::NewStream(conn) => self.d.ack_on_new_stream(conn, &ack, AckOf::Response).await,
        }
    }
}

struct Slot {
    tx: mpsc::UnboundedSender<SlotEvent>,
    order: ResponseOrder,
    max_reply: usize,
    unary: bool,
    state: watch::Sender<SlotState>,
    /// A stream's frames delivered to its owner and not yet recovered by it, by `delivery_seq`:
    /// each frame's ack and the stream its latest copy arrived on.
    unrecovered: HashMap<u64, (TransportHeader, AckTo)>,
}

/// Where a caller's open invocation stands.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct SlotState {
    /// The request was acked (by an `Ack` or by a response frame).
    pub acked: bool,
    /// The invocation is complete: its owner has its terminal frame (or ended it).
    pub complete: bool,
}

enum Sent {
    Request,
    Response { invocation: DeliveryId, seq: u32 },
}

struct Owed {
    reply_to: iroh::PublicKey,
    frames: BTreeMap<u32, (u64, Vec<u8>)>,
    complete: bool,
    pushing: bool,
    kick: Arc<tokio::sync::Notify>,
    acked: watch::Sender<u64>,
    /// The request's W3C context: a resend of its response is a step of the caller's trace.
    parent: Option<(String, Option<String>)>,
}

/// `span` as a child of the remote context `parent` (`traceparent`, `tracestate`), when there is one.
pub(crate) fn parent_on(span: &tracing::Span, parent: Option<&(String, Option<String>)>) {
    if let Some((tp, ts)) = parent {
        crate::set_remote_parent(span, tp, ts.as_deref());
    }
}

/// The remote context a request carried, as [`parent_on`] takes it.
pub(crate) fn parent_of(context: &rdm_core::context::CallContext) -> Option<(String, Option<String>)> {
    context.traceparent.clone().map(|tp| (tp, context.tracestate.clone()))
}

struct St {
    send: SendWindow<iroh::PublicKey>,
    sent: HashMap<DeliveryId, Sent>,
    recv: HashMap<Birth, RecvWindow<Seen>>,
    calls: HashMap<DeliveryId, Slot>,
    done_carried: HashMap<Birth, BTreeSet<u64>>,
    owed: HashMap<DeliveryId, Owed>,
}

struct Inner {
    me: Birth,
    pacing: Pacing,
    st: Mutex<St>,
    conns: Mutex<HashMap<iroh::PublicKey, Vec<Connection>>>,
    conns_changed: watch::Sender<u64>,
    resolver: Mutex<Option<Arc<dyn NodeResolver>>>,
    dialer: Mutex<Option<Dialer>>,
    seams: DeliverySeams,
    stats: DeliveryStats,
}

/// The process's delivery state, shared by its client and its server.
#[derive(Clone)]
pub struct Delivery {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for Delivery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Delivery").field("me", &self.inner.me).finish()
    }
}

/// A process that is no node (a probe, an external client) mints a birth of its own once: its
/// identities need only be unique for its life.
pub(crate) fn minted_birth() -> Birth {
    Birth { node_id: NodeId::mint(), incarnation: IncarnationId::mint() }
}

impl Delivery {
    /// The delivery state of the process that is `me`, resending on the process's cadences.
    pub fn new(me: Birth) -> Self {
        Self::with_pacing(me, Pacing::from_cadences())
    }

    /// The delivery state of `me`, resending on `pacing`.
    pub fn with_pacing(me: Birth, pacing: Pacing) -> Self {
        Self {
            inner: Arc::new(Inner {
                me,
                pacing,
                st: Mutex::new(St {
                    send: SendWindow::new(),
                    sent: HashMap::new(),
                    recv: HashMap::new(),
                    calls: HashMap::new(),
                    done_carried: HashMap::new(),
                    owed: HashMap::new(),
                }),
                conns: Mutex::default(),
                conns_changed: watch::Sender::new(0),
                resolver: Mutex::default(),
                dialer: Mutex::default(),
                seams: DeliverySeams::default(),
                stats: DeliveryStats::default(),
            }),
        }
    }

    /// The birth this process's frames are sent as.
    pub fn me(&self) -> &Birth {
        &self.inner.me
    }

    /// The resend schedule.
    pub fn pacing(&self) -> Pacing {
        self.inner.pacing
    }

    /// The counters.
    pub fn stats(&self) -> &DeliveryStats {
        &self.inner.stats
    }

    /// The test seams.
    pub fn seams(&self) -> &DeliverySeams {
        &self.inner.seams
    }

    /// How many request identities of `sender` the duplicate window holds, and its watermark.
    pub fn window_of(&self, sender: &Birth) -> (usize, u64) {
        let st = self.inner.st.lock().unwrap();
        st.recv.get(sender).map(|w| (w.len(), w.below())).unwrap_or((0, 0))
    }

    /// How many response frames this process owes, over every invocation.
    pub fn owed_frames(&self) -> usize {
        self.inner.st.lock().unwrap().owed.values().map(|o| o.frames.len()).sum()
    }

    /// The resolver that proves a birth's exit, and the dial a resent response takes when no live
    /// connection reaches its caller: handed in by the process's client.
    pub(crate) fn attach_client(&self, resolver: Arc<dyn NodeResolver>, dialer: Dialer) {
        let changes = resolver.changes();
        *self.inner.resolver.lock().unwrap() = Some(resolver);
        *self.inner.dialer.lock().unwrap() = Some(dialer);
        // A sender birth's duplicate window is kept until its exit or removal is proven: each change
        // of the view is when that proof can arrive.
        let (Ok(rt), Some(mut changes)) = (tokio::runtime::Handle::try_current(), changes) else { return };
        let weak = Arc::downgrade(&self.inner);
        rt.spawn(async move {
            while changes.changed().await.is_ok() {
                let Some(inner) = weak.upgrade() else { return };
                Delivery { inner }.forget_ended_senders();
            }
        });
    }

    /// Forget the duplicate window of every sender birth whose exit or removal the view proves.
    fn forget_ended_senders(&self) {
        let senders: Vec<Birth> = self.inner.st.lock().unwrap().recv.keys().cloned().collect();
        for b in senders {
            if let Some(why) = self.birth_ended(&b) {
                let mut st = self.inner.st.lock().unwrap();
                let held = st.recv.remove(&b).map_or(0, |w| w.len());
                st.done_carried.remove(&b);
                drop(st);
                tracing::info_span!("rdm.node_rpc.delivery.remove.via-sender-ended", sender = %b, held, reason = %why)
                    .in_scope(|| tracing::info!("the sender birth ended: its duplicate window is forgotten"));
            }
        }
    }

    /// How many identities this birth has minted.
    pub fn minted(&self) -> u64 {
        self.inner.st.lock().unwrap().send.next() - 1
    }

    // ---- live connections ----

    /// Hold `conn` as a way to reach its peer while it lives.
    pub(crate) fn register(&self, conn: &Connection) {
        let mut conns = self.inner.conns.lock().unwrap();
        let held = conns.entry(conn.remote_id()).or_default();
        if held.iter().any(|c| c.stable_id() == conn.stable_id()) {
            return;
        }
        held.push(conn.clone());
        drop(conns);
        self.inner.conns_changed.send_modify(|v| *v += 1);
        // Held only while it lives: once it closes it leaves the registry, whoever accepted or dialled it.
        let (weak, closing) = (Arc::downgrade(&self.inner), conn.clone());
        tokio::spawn(async move {
            closing.closed().await;
            if let Some(inner) = weak.upgrade() {
                Delivery { inner }.unregister(&closing);
            }
        });
    }

    /// `conn` is closed.
    pub(crate) fn unregister(&self, conn: &Connection) {
        let mut conns = self.inner.conns.lock().unwrap();
        if let Some(held) = conns.get_mut(&conn.remote_id()) {
            held.retain(|c| c.stable_id() != conn.stable_id());
            if held.is_empty() {
                conns.remove(&conn.remote_id());
            }
        }
    }

    fn live_to(&self, peer: &iroh::PublicKey) -> Option<Connection> {
        let conns = self.inner.conns.lock().unwrap();
        conns.get(peer).and_then(|held| held.iter().rev().find(|c| c.close_reason().is_none()).cloned())
    }

    // ---- the sender's window ----

    /// Mint the identity of a frame toward every one of `receivers`.
    pub(crate) fn mint(&self, receivers: &[iroh::PublicKey]) -> DeliveryId {
        let seq = self.inner.st.lock().unwrap().send.mint(receivers);
        DeliveryId { sender: self.inner.me.clone(), delivery_seq: seq }
    }

    /// The identity of a frame toward `receivers` that is written once and never resent: an Owed
    /// request's carried attempt to its carrier (i146 Q52). Settled when the guard drops.
    pub(crate) fn mint_once(&self, receivers: &[iroh::PublicKey]) -> CallGuard {
        CallGuard { d: self.clone(), id: self.mint(receivers) }
    }

    /// The `forget_below` a frame toward `receivers` carries: the lowest over them.
    pub(crate) fn lowest_unacked(&self, receivers: &[iroh::PublicKey]) -> u64 {
        let st = self.inner.st.lock().unwrap();
        receivers.iter().map(|r| st.send.lowest_unacked(r)).min().unwrap_or(1)
    }

    fn settle_own(&self, st: &mut St, id: &DeliveryId) {
        if id.sender == self.inner.me {
            st.send.settle(id.delivery_seq);
        }
    }

    // ---- the caller's open invocations ----

    /// Open the caller's side of invocation `id`: response frames for it, from any stream, reach
    /// the returned receiver in order. The guard ends it (complete or cancelled by its owner) when
    /// dropped.
    pub(crate) fn open_call(&self, id: DeliveryId, max_reply: usize, unary: bool) -> (CallGuard, mpsc::UnboundedReceiver<SlotEvent>, watch::Receiver<SlotState>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let (state, state_rx) = watch::channel(SlotState::default());
        let mut st = self.inner.st.lock().unwrap();
        st.sent.insert(id.clone(), Sent::Request);
        st.calls.insert(id.clone(), Slot { tx, order: ResponseOrder::new(), max_reply, unary, state, unrecovered: HashMap::new() });
        drop(st);
        (CallGuard { d: self.clone(), id }, rx, state_rx)
    }

    /// The owner has the invocation's terminal (or ended it): nothing more is delivered to it, and a
    /// late frame for it is acked and dropped.
    pub(crate) fn complete_call(&self, id: &DeliveryId) {
        let mut st = self.inner.st.lock().unwrap();
        self.end_call(&mut st, id);
    }

    fn end_call(&self, st: &mut St, id: &DeliveryId) {
        st.sent.remove(id);
        self.settle_own(st, id);
        if let Some(slot) = st.calls.remove(id) {
            slot.state.send_modify(|s| s.complete = true);
            if id.sender != self.inner.me {
                st.done_carried.entry(id.sender.clone()).or_default().insert(id.delivery_seq);
            }
        }
    }

    /// The request `id` was acked: its retry ends.
    fn request_acked(&self, st: &mut St, id: &DeliveryId) -> bool {
        let Some(Sent::Request) = st.sent.get(id) else { return false };
        st.sent.remove(id);
        self.settle_own(st, id);
        if let Some(slot) = st.calls.get(id) {
            slot.state.send_modify(|s| s.acked = true);
        }
        true
    }

    /// A response frame arrived (on whatever stream, `to` its way back): deliver it to its open
    /// invocation in order. Returns the acks to write now: a unary invocation's frame is acked as it
    /// is delivered; a stream's frames carry their acks to their owner, which writes each once it can
    /// recover the frame. A copy of a frame its owner holds unrecovered is not delivered again and is
    /// not acked: its ack goes, once the owner recovers it, to the stream this copy arrived on.
    pub(crate) fn on_response(&self, h: &TransportHeader, body: Vec<u8>, to: AckTo) -> ResponseVerdict {
        let TransportHeader::Response { sender, invocation_id, seq, delivery_seq, .. } = h else { return ResponseVerdict::Violation("not a response frame".into()) };
        let mut st = self.inner.st.lock().unwrap();
        // Any response frame acks the request it answers.
        self.request_acked(&mut st, invocation_id);
        let Some(slot) = st.calls.get_mut(invocation_id) else {
            return if self.completed(&st, invocation_id) {
                self.inner.stats.responses_of_completed.fetch_add(1, Ordering::SeqCst);
                ResponseVerdict::Ack(vec![h.ack()])
            } else {
                self.inner.stats.responses_unheld.fetch_add(1, Ordering::SeqCst);
                ResponseVerdict::Drop
            };
        };
        match slot.order.admit(*seq, *delivery_seq, body) {
            Ok(Admitted::Deliver(frames)) => {
                let unary = slot.unary;
                let ends = unary && frames.iter().any(|(s, _, _)| *s == 0);
                let mut now = Vec::new();
                for (seq, d, b) in frames {
                    let ack = TransportHeader::Ack { sender: sender.clone(), delivery_seq: d };
                    let later = if unary {
                        now.push(ack);
                        None
                    } else {
                        slot.unrecovered.insert(d, (ack, to.clone()));
                        Some(AckOnRecovery { d: self.clone(), invocation: invocation_id.clone(), seq, delivery_seq: d })
                    };
                    let _ = slot.tx.send(SlotEvent::Frame(b, later));
                }
                if ends {
                    self.end_call(&mut st, invocation_id);
                }
                ResponseVerdict::Ack(now)
            }
            Ok(Admitted::Duplicate) => match slot.unrecovered.get_mut(delivery_seq) {
                Some(held) => {
                    held.1 = to;
                    ResponseVerdict::Held
                }
                None => ResponseVerdict::Ack(vec![h.ack()]),
            },
            // A frame held only in the reorder buffer is not acknowledged.
            Ok(Admitted::Held) => ResponseVerdict::Held,
            Err(v) => {
                let _ = slot.tx.send(SlotEvent::Violation(v.to_string()));
                self.inner.stats.violations.fetch_add(1, Ordering::SeqCst);
                ResponseVerdict::Violation(v.to_string())
            }
        }
    }

    /// The invocation `id`'s response broke the framing on a stream: its owner is told, `424`.
    /// Whether invocation `id` holds a frame handed to its owner and not yet recovered whose ack is
    /// written on `out` (node-rpc.md §31, §43): that direction is still needed for the ack.
    pub(crate) fn unrecovered_on(&self, id: &DeliveryId, out: &FrameOut) -> bool {
        self.inner.st.lock().unwrap().calls.get(id).is_some_and(|s| s.unrecovered.values().any(|(_, to)| matches!(to, AckTo::Stream(o) if o.same(out))))
    }

    pub(crate) fn fail_call(&self, id: &DeliveryId, why: &str) {
        self.inner.stats.violations.fetch_add(1, Ordering::SeqCst);
        if let Some(slot) = self.inner.st.lock().unwrap().calls.get(id) {
            let _ = slot.tx.send(SlotEvent::Violation(why.to_string()));
        }
    }

    /// The bound a response body for `invocation` is read within.
    pub(crate) fn reply_bound(&self, invocation: &DeliveryId) -> usize {
        self.inner.st.lock().unwrap().calls.get(invocation).map(|s| s.max_reply).unwrap_or(MAX_UNHELD_REPLY_BYTES)
    }

    /// Has the invocation `id`, which this process does not hold open, completed? Its own: minted and
    /// no longer open. A carried one: completed here and not yet forgotten. Another birth of this
    /// node: that receiving birth has ended. Anything else was never held.
    fn completed(&self, st: &St, id: &DeliveryId) -> bool {
        let me = &self.inner.me;
        if id.sender == *me {
            return id.delivery_seq < st.send.next() && !st.calls.contains_key(id);
        }
        if id.sender.node_id == me.node_id {
            return true;
        }
        st.done_carried.get(&id.sender).is_some_and(|s| s.contains(&id.delivery_seq))
    }

    // ---- acks ----

    /// An `Ack` arrived: settle the request or response frame it names.
    pub(crate) fn on_ack(&self, h: &TransportHeader) {
        let id = h.delivery_id();
        let mut st = self.inner.st.lock().unwrap();
        if self.request_acked(&mut st, &id) {
            self.inner.stats.acks_received.fetch_add(1, Ordering::SeqCst);
            return;
        }
        let Some(Sent::Response { invocation, seq }) = st.sent.remove(&id) else { return };
        self.inner.stats.acks_received.fetch_add(1, Ordering::SeqCst);
        self.settle_own(&mut st, &id);
        let ended = match st.owed.get_mut(&invocation) {
            Some(o) => {
                o.frames.remove(&seq);
                o.acked.send_modify(|v| *v += 1);
                o.complete && o.frames.is_empty()
            }
            None => false,
        };
        if ended {
            st.owed.remove(&invocation);
        }
    }

    /// Write the ack `a` of a response frame on `out`, unless the seam drops it.
    pub(crate) async fn write_ack_header(&self, out: &FrameOut, a: &TransportHeader) -> bool {
        self.write_ack(out, a, AckOf::Response).await
    }

    /// Write `h`'s `Ack` on `out`, unless the seam drops it.
    pub(crate) async fn write_ack(&self, out: &FrameOut, h: &TransportHeader, kind: AckOf) -> bool {
        let seam = match kind {
            AckOf::Request => &self.inner.seams.drop_next_request_ack,
            AckOf::Response => &self.inner.seams.drop_next_response_ack,
        };
        if DeliverySeams::take(seam) {
            tracing::info_span!("rdm.node_rpc.delivery.remove.via-ack-dropped-by-seam", delivery_id = %h.delivery_id(), of = kind.name())
                .in_scope(|| tracing::info!("the test seam dropped this ack"));
            if kind == AckOf::Response {
                // The stream dies with the ack: the responder sees its request direction reset.
                out.reset(ResetCode::RequestStop).await;
            }
            return false;
        }
        let held = kind == AckOf::Request && DeliverySeams::take(&self.inner.seams.hold_next_request_ack);
        if held {
            tracing::info_span!("rdm.node_rpc.delivery.update.via-ack-held-by-seam", delivery_id = %h.delivery_id(), of = kind.name())
                .in_scope(|| tracing::info!("the test seam holds this ack until the cell releases it"));
            self.inner.seams.request_ack_released.notified().await;
        }
        let written = out.send(&encode_ack_frame(&h.ack())).await;
        if held {
            tracing::info_span!("rdm.node_rpc.delivery.update.via-held-ack-released", delivery_id = %h.delivery_id(), of = kind.name(), written = ?written)
                .in_scope(|| tracing::info!("the test seam released the held ack: its write ended"));
            self.inner.seams.request_ack_written.notify_one();
        }
        let ok = written == Written::Ok;
        if ok {
            self.inner.stats.acks_sent.fetch_add(1, Ordering::SeqCst);
        } else {
            tracing::info_span!("rdm.node_rpc.delivery.reject.via-ack-not-written", delivery_id = %h.delivery_id(), of = kind.name(), written = ?written)
                .in_scope(|| tracing::info!("the ack's stream took no write"));
        }
        ok
    }

    // ---- the receiver's duplicate window ----

    /// A request frame `h` arrived: is it new, or a repeat of one held?
    pub(crate) fn arrive_request(&self, h: &TransportHeader) -> RequestArrival {
        let TransportHeader::Request { sender, delivery_seq, forget_below } = h else { unreachable!("arrive_request takes a request header") };
        let mut st = self.inner.st.lock().unwrap();
        let window = st.recv.entry(sender.clone()).or_default();
        let arrival = match window.arrive(*delivery_seq, *forget_below) {
            Arrival::New => {
                window.insert(*delivery_seq, Seen::Pending(Vec::new()));
                RequestArrival::New
            }
            Arrival::Held(Seen::Pending(waiters)) => {
                let (tx, rx) = oneshot::channel();
                waiters.push(tx);
                RequestArrival::Pending(rx)
            }
            Arrival::Held(Seen::Accepted) | Arrival::BelowWatermark => RequestArrival::Accepted,
            Arrival::Held(Seen::Ended(code)) => RequestArrival::Ended(*code),
        };
        let below = st.recv.get(sender).map(|w| w.below()).unwrap_or(0);
        if let Some(done) = st.done_carried.get_mut(sender) {
            *done = done.split_off(&below);
        }
        match &arrival {
            RequestArrival::New => self.inner.stats.requests_new.fetch_add(1, Ordering::SeqCst),
            RequestArrival::Pending(_) => self.inner.stats.duplicates_pending.fetch_add(1, Ordering::SeqCst),
            RequestArrival::Accepted | RequestArrival::Ended(_) => self.inner.stats.duplicates_acked.fetch_add(1, Ordering::SeqCst),
        };
        arrival
    }

    /// Settle the request `id`'s entry: accepted, ended with a reset code, or dropped undispatched.
    pub(crate) fn settle_request(&self, id: &DeliveryId, how: Settled) {
        let mut st = self.inner.st.lock().unwrap();
        let Some(window) = st.recv.get_mut(&id.sender) else { return };
        let Some(seen) = window.get_mut(id.delivery_seq) else { return };
        let waiters = match std::mem::replace(
            seen,
            match &how {
                Settled::Accepted => Seen::Accepted,
                Settled::Ended(code) => Seen::Ended(*code),
                Settled::Dropped => Seen::Ended(0),
            },
        ) {
            Seen::Pending(w) => w,
            _ => Vec::new(),
        };
        if how == Settled::Dropped {
            // Never dispatched: forget it, so the sender's next resend is a first arrival.
            window.remove(id.delivery_seq);
        }
        drop(st);
        for w in waiters {
            let _ = w.send(how.clone());
        }
    }

    // ---- owed responses ----

    /// Open the owed response of invocation `id`, answered toward `reply_to`.
    pub(crate) fn owe_response(&self, id: &DeliveryId, reply_to: iroh::PublicKey) -> watch::Receiver<u64> {
        self.owe_response_in(id, reply_to, None)
    }

    /// [`Self::owe_response`] for a request that carried the remote context `parent`.
    pub(crate) fn owe_response_in(&self, id: &DeliveryId, reply_to: iroh::PublicKey, parent: Option<(String, Option<String>)>) -> watch::Receiver<u64> {
        let mut st = self.inner.st.lock().unwrap();
        let o = st.owed.entry(id.clone()).or_insert_with(|| Owed {
            reply_to,
            frames: BTreeMap::new(),
            complete: false,
            pushing: false,
            kick: Arc::new(tokio::sync::Notify::new()),
            acked: watch::Sender::new(0),
            parent: None,
        });
        o.reply_to = reply_to;
        if parent.is_some() {
            o.parent = parent;
        }
        o.acked.subscribe()
    }

    /// Record the response frame at `seq` of invocation `id` as owed, and return its encoded frame:
    /// `None` when its response is owed no longer (its caller ended the invocation), and nothing is
    /// to be written.
    pub(crate) fn owe_frame(&self, id: &DeliveryId, seq: u32, body: Vec<u8>, terminal: bool) -> Option<Vec<u8>> {
        let mut st = self.inner.st.lock().unwrap();
        let reply_to = st.owed.get(id).map(|o| o.reply_to)?;
        let delivery_seq = st.send.mint(&[reply_to]);
        let mine = DeliveryId { sender: self.inner.me.clone(), delivery_seq };
        st.sent.insert(mine, Sent::Response { invocation: id.clone(), seq });
        let forget_below = st.send.lowest_unacked(&reply_to);
        let o = st.owed.get_mut(id).expect("checked above");
        let h = TransportHeader::Response { sender: self.inner.me.clone(), delivery_seq, forget_below, invocation_id: id.clone(), seq };
        let frame = encode_response_frame(&h, &body);
        o.frames.insert(seq, (delivery_seq, body));
        o.complete |= terminal;
        Some(frame)
    }

    /// How many frames of `id`'s response are owed.
    pub(crate) fn owed_frames_of(&self, id: &DeliveryId) -> usize {
        self.inner.st.lock().unwrap().owed.get(id).map_or(0, |o| o.frames.len())
    }

    /// Whether every frame of `id`'s owed response is acked (or it was never owed).
    pub(crate) fn response_settled(&self, id: &DeliveryId) -> bool {
        self.inner.st.lock().unwrap().owed.get(id).is_none_or(|o| o.frames.is_empty())
    }

    /// Ticks on every ack of `id`'s response; ends (an error) once the response is complete and
    /// acked, or dropped. `None` when nothing is owed for it.
    pub(crate) fn acked_watch(&self, id: &DeliveryId) -> Option<watch::Receiver<u64>> {
        self.inner.st.lock().unwrap().owed.get(id).map(|o| o.acked.subscribe())
    }

    /// The invocation ended in a reset: nothing of its response is owed.
    pub(crate) fn drop_response(&self, id: &DeliveryId) {
        self.inner.st.lock().unwrap().owed.remove(id);
    }

    /// Resend `id`'s owed response after `delay`, unless the caller has acked it by then.
    pub(crate) fn push_after(&self, id: &DeliveryId, delay: Duration) {
        let (weak, id) = (Arc::downgrade(&self.inner), id.clone());
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            if let Some(inner) = weak.upgrade() {
                let d = Delivery { inner };
                if !d.response_settled(&id) {
                    d.push(&id, None, "no ack within the first resend delay of a refusal whose request half was stopped");
                }
            }
        });
    }

    /// The request stream of `id` is gone (or a repeat proved its caller reachable on another
    /// connection, `reply_to`): resend whatever of its response is still owed, on a new stream.
    /// The caller of `id` is reachable at `reply_to` now (a repeat arrived from there): a response
    /// owed later, or resent later, goes there. Nothing is resent for it: the stream the response
    /// was written on carries it while it lives, and its end resends what it left unacked.
    pub(crate) fn answer_to(&self, id: &DeliveryId, reply_to: iroh::PublicKey) {
        if let Some(o) = self.inner.st.lock().unwrap().owed.get_mut(id) {
            o.reply_to = reply_to;
        }
    }

    pub(crate) fn push(&self, id: &DeliveryId, reply_to: Option<iroh::PublicKey>, why: &str) {
        let mut st = self.inner.st.lock().unwrap();
        let Some(o) = st.owed.get_mut(id) else { return };
        if let Some(r) = reply_to {
            o.reply_to = r;
        }
        if o.frames.is_empty() {
            // Nothing owed yet: the frames to come are written where the caller now is.
            if o.complete {
                st.owed.remove(id);
            }
            return;
        }
        if o.pushing {
            o.kick.notify_one();
            return;
        }
        o.pushing = true;
        let parent = o.parent.clone();
        drop(st);
        // The loop holds the delivery state weakly: a process that ends its server ends its resends.
        tokio::spawn(push_loop(Arc::downgrade(&self.inner), id.clone(), why.to_string(), parent));
    }

    fn birth_ended(&self, birth: &Birth) -> Option<String> {
        let resolver = self.inner.resolver.lock().unwrap().clone()?;
        match resolver.resolve(&NodeTarget::ExactNode(birth.node_id.clone())) {
            Ok(n) if n.incarnation == birth.incarnation => None,
            Ok(n) => Some(format!("birth {} superseded by incarnation {}", birth, n.incarnation.as_str())),
            Err(rdm_core::outcome::ResolveFailure::Gone) => Some(format!("birth {birth} removed")),
            // The live resolver answers `Unavailable` for exactly a birth whose exit is proven.
            Err(rdm_core::outcome::ResolveFailure::Unavailable) => Some(format!("birth {birth} exited: its exit is proven")),
            Err(_) => None,
        }
    }

    /// One resend of `id`'s owed response: `Err` ends the loop with its outcome, `Ok`
    /// waits on what it returns before the next resend.
    async fn push_resend(&self, id: &DeliveryId, attempt: u32, span: &tracing::Span) -> Result<(Arc<tokio::sync::Notify>, watch::Receiver<u64>), &'static str> {
        let (reply_to, frames, kick) = {
            let mut st = self.inner.st.lock().unwrap();
            let Some(o) = st.owed.get_mut(id) else { return Err("acked") };
            if o.frames.is_empty() {
                if o.complete {
                    st.owed.remove(id);
                    return Err("acked");
                }
                o.pushing = false;
                return Err("caught-up");
            }
            (o.reply_to, o.frames.iter().map(|(s, (d, b))| (*s, *d, b.clone())).collect::<Vec<_>>(), o.kick.clone())
        };
        if let Some(why) = self.birth_ended(&id.sender) {
            self.inner.st.lock().unwrap().owed.remove(id);
            span.in_scope(|| tracing::info!(reason = %why, "the receiving birth ended: the owed response is dropped"));
            return Err("receiver-gone");
        }
        let conns = self.inner.conns_changed.subscribe();
        let (conn, via) = match self.live_to(&reply_to) {
            Some(c) => (Some(c), "live-connection"),
            None => {
                let dial = self.inner.dialer.lock().unwrap().clone();
                match dial {
                    Some(dial) => (dial(reply_to).await, "ordinary-dial"),
                    None => (None, "no-connection"),
                }
            }
        };
        tracing::info!(parent: span, attempt, frames = frames.len(), via, peer = %reply_to.fmt_short(), "owed response frames resent on a new stream");
        if let Some(conn) = conn {
            self.resend_frames(id, &conn, frames).instrument_in(span).await;
            // Acked on that stream: the response is owed no longer, said now, not a resend later.
            if !self.inner.st.lock().unwrap().owed.contains_key(id) {
                return Err("acked");
            }
        }
        Ok((kick, conns))
    }

    async fn resend_frames(&self, id: &DeliveryId, conn: &Connection, frames: Vec<(u32, u64, Vec<u8>)>) {
        let Ok((send, recv)) = conn.open_bi().await else { return };
        let out = FrameOut::new(send);
        let sent: Vec<u32> = frames.iter().map(|(s, _, _)| *s).collect();
        for (seq, delivery_seq, body) in frames {
            let forget_below = self.lowest_unacked(&[conn.remote_id()]);
            let h = TransportHeader::Response { sender: self.inner.me.clone(), delivery_seq, forget_below, invocation_id: id.clone(), seq };
            if !out.write(&encode_response_frame(&h, &body)).await {
                return;
            }
            self.inner.stats.responses_resent.fetch_add(1, Ordering::SeqCst);
        }
        // The caller's acks come back on this stream's other direction; read them until it ends or
        // every frame this resend sent again is acked (frames written since go out on their own stream).
        let mut acked = match self.inner.st.lock().unwrap().owed.get(id) {
            Some(o) => o.acked.subscribe(),
            None => return,
        };
        let mut reader = FrameReader::new(recv, Vec::new());
        loop {
            let pending = self.inner.st.lock().unwrap().owed.get(id).is_some_and(|o| sent.iter().any(|s| o.frames.contains_key(s)));
            if !pending {
                out.finish().await;
                return;
            }
            tokio::select! {
                h = reader.header() => match h {
                    Ok(Some(h @ TransportHeader::Ack { .. })) => self.on_ack(&h),
                    _ => return,
                },
                _ = acked.changed() => {}
            }
        }
    }

    // ---- streams this layer serves ----

    /// Serve a stream whose first frame is a `Response` or an `Ack` (`first`), on a connection from
    /// `peer`: resent response frames or acks, demultiplexed before any dispatch or admission.
    pub(crate) async fn serve_reverse_stream(&self, first: TransportHeader, send: SendStream, reader: FrameReader) {
        let out = FrameOut::new(send);
        let mut reader = reader;
        let mut next = Some(first);
        loop {
            let h = match next.take() {
                Some(h) => h,
                None => match reader.header().await {
                    Ok(Some(h)) => h,
                    Ok(None) => break,
                    Err(ReadEnd::Violation(why)) => {
                        self.violation(&out, &mut reader, why).await;
                        return;
                    }
                    Err(_) => break,
                },
            };
            match &h {
                TransportHeader::Ack { .. } => self.on_ack(&h),
                TransportHeader::Response { invocation_id, .. } => {
                    let bound = self.reply_bound(invocation_id);
                    let body = match reader.body(bound).await {
                        Ok(b) => b,
                        Err(ReadEnd::Violation(why)) => {
                            self.violation(&out, &mut reader, why).await;
                            return;
                        }
                        Err(_) => break,
                    };
                    match self.on_response(&h, body, AckTo::Stream(out.clone())) {
                        ResponseVerdict::Ack(acks) => {
                            for a in acks {
                                self.write_ack_header(&out, &a).await;
                            }
                        }
                        ResponseVerdict::Held | ResponseVerdict::Drop => {}
                        ResponseVerdict::Violation(_) => {
                            self.violation(&out, &mut reader, "a response frame broke its invocation's order").await;
                            return;
                        }
                    }
                }
                TransportHeader::Request { .. } => {
                    self.violation(&out, &mut reader, "a request frame after the stream's first frame").await;
                    return;
                }
            }
        }
        out.finish().await;
    }

    async fn violation(&self, out: &FrameOut, reader: &mut FrameReader, why: &'static str) {
        self.inner.stats.violations.fetch_add(1, Ordering::SeqCst);
        tracing::info_span!("rdm.node_rpc.stream.reject.via-protocol-violation", reason = why).in_scope(|| tracing::info!("424"));
        out.reset(ResetCode::ProtocolViolation).await;
        reader.stop(ResetCode::ProtocolViolation);
    }

    /// Write an `Ack` of `h` on a new stream of `conn` (the stream `h` arrived on cannot carry it).
    pub(crate) async fn ack_on_new_stream(&self, conn: &Connection, h: &TransportHeader, kind: AckOf) {
        if let Ok((send, _recv)) = conn.open_bi().await {
            let out = FrameOut::new(send);
            self.write_ack(&out, h, kind).await;
            out.finish().await;
        }
    }

    /// Accept the streams the peer opens on `conn`, a connection this process dialed: resent
    /// responses and acks (a request never arrives on a connection this process dialed).
    pub(crate) fn serve_dialed(&self, conn: Connection) {
        self.register(&conn);
        let d = self.clone();
        tokio::spawn(async move {
            while let Ok((send, recv)) = conn.accept_bi().await {
                let d = d.clone();
                tokio::spawn(async move {
                    let mut reader = FrameReader::new(recv, Vec::new());
                    let out_send = send;
                    match reader.header().await {
                        Ok(Some(h @ (TransportHeader::Response { .. } | TransportHeader::Ack { .. }))) => d.serve_reverse_stream(h, out_send, reader).await,
                        Ok(Some(TransportHeader::Request { .. })) => {
                            let out = FrameOut::new(out_send);
                            d.violation(&out, &mut reader, "a request on a connection this process dialed").await;
                        }
                        Err(ReadEnd::Violation(why)) => {
                            let out = FrameOut::new(out_send);
                            d.violation(&out, &mut reader, why).await;
                        }
                        _ => {}
                    }
                });
            }
            d.unregister(&conn);
        });
    }
}

/// Resend `id`'s owed response, on the schedule, until it is acked, its receiving birth ends, or the
/// process's delivery state is gone.
async fn push_loop(weak: std::sync::Weak<Inner>, id: DeliveryId, why: String, parent: Option<(String, Option<String>)>) {
    let span = tracing::info_span!("rdm.node_rpc.delivery.update.via-response-resend", delivery_id = %id, reason = %why, attempts = tracing::field::Empty, outcome = tracing::field::Empty);
    parent_on(&span, parent.as_ref());
    let mut attempt = 0u32;
    let outcome = loop {
        let Some(inner) = weak.upgrade() else { break "process-ended" };
        let d = Delivery { inner };
        attempt += 1;
        let (kick, mut conns) = match d.push_resend(&id, attempt, &span).await {
            Ok(w) => w,
            Err(outcome) => break outcome,
        };
        let wait = d.inner.pacing.after(attempt);
        drop(d);
        tokio::select! {
            _ = tokio::time::sleep(wait) => {}
            _ = kick.notified() => {}
            _ = conns.changed() => {}
        }
    };
    span.record("attempts", attempt);
    span.record("outcome", outcome);
    if attempt > 1 || outcome != "caught-up" {
        span.in_scope(|| tracing::info!("owed response frames"));
    }
}

/// A response frame's fate at the caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ResponseVerdict {
    /// Delivered (or a repeat, or for a completed invocation): these acks are written now (none for
    /// a stream's frames, whose owner acks them as it reads them).
    Ack(Vec<TransportHeader>),
    /// Held past a missing position: not acked yet.
    Held,
    /// For an invocation this process never held: neither delivered nor acked.
    Drop,
    /// The frame broke its invocation's order: `424`.
    Violation(String),
}

/// Which frame an ack acknowledges, for the seams and the spans.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AckOf {
    Request,
    Response,
}

impl AckOf {
    fn name(self) -> &'static str {
        match self {
            Self::Request => "request",
            Self::Response => "response",
        }
    }
}

/// What a request's arrival is.
pub(crate) enum RequestArrival {
    /// First seen: it is held pending until it is accepted, ended or dropped.
    New,
    /// A repeat while the original has not reached its acceptance boundary.
    Pending(oneshot::Receiver<Settled>),
    /// A repeat after acceptance: ack it, end its stream.
    Accepted,
    /// A repeat of a request that ended in a reset.
    Ended(u64),
}

/// The caller's hold on an open invocation: dropped, it ends the invocation (complete, or cancelled
/// by its owner) and its request's retry.
pub(crate) struct CallGuard {
    d: Delivery,
    id: DeliveryId,
}

impl CallGuard {
    /// The owner has the invocation's terminal: it is complete.
    pub(crate) fn complete(&self) {
        self.d.complete_call(&self.id);
    }

    pub(crate) fn id(&self) -> &DeliveryId {
        &self.id
    }
}

impl Drop for CallGuard {
    fn drop(&mut self) {
        self.d.complete_call(&self.id);
    }
}

/// The acceptance boundary of one dispatched request (node-rpc.md §43): its handler says when the
/// accepted work is recoverable; a read-only request is accepted as it is dispatched; otherwise its
/// first response frame accepts it.
#[derive(Clone)]
pub struct Acceptance {
    inner: Arc<AcceptanceInner>,
}

struct AcceptanceInner {
    accepted: AtomicBool,
    tx: watch::Sender<bool>,
}

impl std::fmt::Debug for Acceptance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Acceptance").field("accepted", &self.inner.accepted.load(Ordering::SeqCst)).finish()
    }
}

impl Acceptance {
    pub(crate) fn new() -> (Self, watch::Receiver<bool>) {
        let (tx, rx) = watch::channel(false);
        (Self { inner: Arc::new(AcceptanceInner { accepted: AtomicBool::new(false), tx }) }, rx)
    }

    /// An acceptance that is no delivery's: a request handled in process, never over the wire.
    pub fn detached() -> Self {
        Self::new().0
    }

    /// The accepted work is recoverable (its acceptance record, or its result, is durable): the
    /// request is acknowledged, and a repeat of it is acked without dispatch. Idempotent.
    pub fn accepted(&self) {
        if !self.inner.accepted.swap(true, Ordering::SeqCst) {
            self.inner.tx.send_replace(true);
        }
    }

    /// Whether the request has been accepted.
    pub fn is_accepted(&self) -> bool {
        self.inner.accepted.load(Ordering::SeqCst)
    }
}

/// One direction of a stream that frames are written to, shared by the writers of one invocation.
/// Once the direction ends (a failed write, its finish, its reset, its take) it keeps why, so a
/// writer that comes after is told what ended it.
#[derive(Clone)]
pub(crate) struct FrameOut(Arc<tokio::sync::Mutex<Result<SendStream, String>>>);

impl FrameOut {
    pub(crate) fn new(send: SendStream) -> Self {
        Self(Arc::new(tokio::sync::Mutex::new(Ok(send))))
    }

    /// Whether `other` is this direction (a clone of it).
    pub(crate) fn same(&self, other: &FrameOut) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    /// End the direction for `why`, handing back the stream if it was still open; an ended
    /// direction keeps the reason it first ended for.
    fn end(dir: &mut Result<SendStream, String>, why: impl FnOnce() -> String) -> Option<SendStream> {
        if dir.is_err() {
            return None;
        }
        std::mem::replace(dir, Err(why())).ok()
    }

    /// Write `bytes`; `false` when the direction is gone.
    pub(crate) async fn write(&self, bytes: &[u8]) -> bool {
        self.send(bytes).await == Written::Ok
    }

    /// Write `bytes`, naming how a failure ended the direction.
    pub(crate) async fn send(&self, bytes: &[u8]) -> Written {
        let mut g = self.0.lock().await;
        let send = match g.as_mut() {
            Ok(send) => send,
            Err(why) => return Written::Gone(format!("the direction is closed: {why}")),
        };
        match send.write_all(bytes).await {
            Ok(()) => Written::Ok,
            Err(e) => {
                let written = match e {
                    iroh::endpoint::WriteError::Stopped(code) => Written::Stopped(code.into_inner()),
                    other => Written::Gone(other.to_string()),
                };
                *g = Err(format!("a write on it failed: {written:?}"));
                written
            }
        }
    }

    /// Finish the direction (FIN): this stream carries nothing more.
    pub(crate) async fn finish(&self) {
        if let Some(mut send) = Self::end(&mut *self.0.lock().await, || "it was finished".into()) {
            let _ = send.finish();
        }
    }

    /// Reset the direction with `code`.
    pub(crate) async fn reset(&self, code: ResetCode) {
        if let Some(mut send) = Self::end(&mut *self.0.lock().await, || format!("it was reset {}", code.code())) {
            let _ = send.reset(VarInt::from_u32(code.code()));
        }
    }

    /// Take the direction back (the caller of a withheld reply writes it itself), or why it ended.
    pub(crate) async fn take(&self) -> Result<SendStream, String> {
        let mut g = self.0.lock().await;
        if let Err(why) = &*g {
            return Err(why.clone());
        }
        Ok(Self::end(&mut g, || "it was taken by a withheld reply's writer".into()).expect("the direction is open"))
    }
}

/// How a write ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Written {
    /// Written.
    Ok,
    /// The peer stopped reading with this code: `0` is a caller that dropped its invocation (its
    /// owner's cancellation); `422` an attempt it abandoned before a resend.
    Stopped(u64),
    /// The stream or its connection is gone.
    Gone(String),
}

/// How reading a stream's frames ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ReadEnd {
    /// The stream ended inside a frame.
    Eof,
    /// The peer reset the direction with this code.
    Reset(u64),
    /// The connection or the stream is gone.
    Lost(String),
    /// A frame broke the framing: `424`.
    Violation(&'static str),
}

/// A stream's incoming frames, header first.
pub(crate) struct FrameReader {
    recv: RecvStream,
    buf: Vec<u8>,
}

impl FrameReader {
    pub(crate) fn new(recv: RecvStream, buf: Vec<u8>) -> Self {
        Self { recv, buf }
    }

    /// Read one chunk into the buffer: `Ok(false)` at the end of the stream.
    async fn fill(&mut self) -> Result<bool, ReadEnd> {
        let mut chunk = [0u8; 16 * 1024];
        match self.recv.read(&mut chunk).await {
            Ok(Some(n)) => {
                self.buf.extend_from_slice(&chunk[..n]);
                Ok(true)
            }
            Ok(None) => Ok(false),
            Err(ReadError::Reset(c)) => Err(ReadEnd::Reset(c.into_inner())),
            Err(e) => Err(ReadEnd::Lost(e.to_string())),
        }
    }

    /// The next frame's header: `None` when the stream ended cleanly between frames.
    pub(crate) async fn header(&mut self) -> Result<Option<TransportHeader>, ReadEnd> {
        loop {
            match decode_header(&self.buf) {
                HeaderRead::Read(h, used) => {
                    self.buf.drain(..used);
                    return Ok(Some(h));
                }
                HeaderRead::Bad(why) => return Err(ReadEnd::Violation(why)),
                HeaderRead::NeedMore => {}
            }
            if !self.fill().await? {
                return if self.buf.is_empty() { Ok(None) } else { Err(ReadEnd::Eof) };
            }
        }
    }

    /// A response frame's `reply_len | reply`, within `max`.
    pub(crate) async fn body(&mut self, max: usize) -> Result<Vec<u8>, ReadEnd> {
        loop {
            match decode_varint(&self.buf) {
                Varint::Complete { value, .. } if value > max as u64 => return Err(ReadEnd::Violation("a response frame over its reply ceiling")),
                Varint::Complete { value, len } if self.buf.len() >= len + value as usize => {
                    let body = self.buf[len..len + value as usize].to_vec();
                    self.buf.drain(..len + value as usize);
                    return Ok(body);
                }
                Varint::Overflow => return Err(ReadEnd::Violation("a response frame's length is not a varint")),
                _ => {}
            }
            if !self.fill().await? {
                return Err(ReadEnd::Eof);
            }
        }
    }

    /// The bytes read and not yet consumed, and the stream.
    pub(crate) fn into_parts(self) -> (RecvStream, Vec<u8>) {
        (self.recv, self.buf)
    }

    /// Stop the direction with `code`.
    pub(crate) fn stop(&mut self, code: ResetCode) {
        let _ = self.recv.stop(VarInt::from_u32(code.code()));
    }
}

trait InstrumentIn: Future + Sized {
    fn instrument_in(self, span: &tracing::Span) -> tracing::instrument::Instrumented<Self> {
        tracing::Instrument::instrument(self, span.clone())
    }
}

impl<F: Future> InstrumentIn for F {}

#[cfg(test)]
mod tests {
    use super::*;
    use rdm_core::{IncarnationId, NodeId};

    /// CONTRACT (i146 Q52; node-rpc-envelope.md transport frames): an Owed request's carried attempt is
    /// a frame of its own toward the carrier, written once: while it is in flight it holds the sender's
    /// `forget_below` toward that carrier at its identity, and once its guard drops it is settled, so
    /// `forget_below` moves past it and the carrier forgets it; the request it carried stays unacked
    /// toward its receiver. Must NOT happen: a finished carried attempt pinning the carrier's duplicate
    /// window, or its settling touching the carried request.
    #[test]
    fn a_carried_attempts_own_identity_is_settled_when_its_guard_drops() {
        let d = Delivery::new(Birth { node_id: NodeId::mint(), incarnation: IncarnationId::mint() });
        let (receiver, carrier) = (iroh::SecretKey::generate().public(), iroh::SecretKey::generate().public());
        let owed = d.mint(&[receiver]);
        let attempt = d.mint_once(&[carrier]);
        assert_eq!(d.lowest_unacked(&[carrier]), attempt.id().delivery_seq, "the attempt in flight holds forget_below toward its carrier");
        drop(attempt);
        assert_eq!(d.lowest_unacked(&[carrier]), d.minted() + 1, "the finished attempt is settled: forget_below toward the carrier moves past it");
        assert_eq!(d.lowest_unacked(&[receiver]), owed.delivery_seq, "the carried request stays unacked toward its receiver");
    }
}
