//! The delivery layer's rules, with no clock and no transport (node-rpc.md §43, §33;
//! node-rpc-envelope.md "Transport frames"): the resend schedule, the sending birth's window, the
//! receiver's duplicate window and the order of one invocation's response frames. The runtime
//! (`rdm_transport::delivery`) drives them.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::Hash;
use std::time::Duration;

/// How far past the next expected position a response frame may arrive and be held.
pub const MAX_REORDER_POSITIONS: u32 = 64;

/// The delay before resend `attempt` (1 is the first resend): the publish cadence, doubling with each
/// attempt, scaled by `jitter` (clamped to ±25 %), never past the staleness floor.
pub fn resend_after(attempt: u32, cadence: Duration, floor: Duration, jitter: f64) -> Duration {
    let factor = 1u32.checked_shl(attempt.saturating_sub(1)).unwrap_or(u32::MAX);
    let base = cadence.saturating_mul(factor).min(floor);
    base.mul_f64(1.0 + jitter.clamp(-0.25, 0.25)).min(floor)
}

/// The sending birth's one `delivery_seq` counter, and the frames it still holds unacked toward each
/// receiver. One counter for every receiver, so an identity is unique at every hop a carried call
/// takes; each receiver sees its own frames in increasing order.
#[derive(Debug)]
pub struct SendWindow<R> {
    next: u64,
    unacked: HashMap<R, BTreeSet<u64>>,
}

impl<R: Eq + Hash + Clone> Default for SendWindow<R> {
    fn default() -> Self {
        Self { next: 1, unacked: HashMap::new() }
    }
}

impl<R: Eq + Hash + Clone> SendWindow<R> {
    /// A new birth's window: its first `delivery_seq` is 1.
    pub fn new() -> Self {
        Self::default()
    }

    /// Mint the next `delivery_seq` for a frame every one of `receivers` may receive (a carried call
    /// reaches its carrier and its final target), held unacked toward each until [`Self::settle`].
    pub fn mint(&mut self, receivers: &[R]) -> u64 {
        let seq = self.next;
        self.next += 1;
        for r in receivers {
            self.unacked.entry(r.clone()).or_default().insert(seq);
        }
        seq
    }

    /// The frame `seq` is owed no longer (acked, its receiver gone, or cancelled by its owner).
    /// Returns whether any receiver still held it.
    pub fn settle(&mut self, seq: u64) -> bool {
        let mut held = false;
        self.unacked.retain(|_, s| {
            held |= s.remove(&seq);
            !s.is_empty()
        });
        held
    }

    /// The `retire_below` a frame toward `receiver` carries: the lowest `delivery_seq` still held
    /// unacked toward it, or the next to be minted. Nothing below it is ever resent to `receiver`.
    pub fn lowest_unacked(&self, receiver: &R) -> u64 {
        self.unacked.get(receiver).and_then(|s| s.first().copied()).unwrap_or(self.next)
    }

    /// The next `delivery_seq` to be minted: everything below it was minted.
    pub fn next(&self) -> u64 {
        self.next
    }

    /// The frames held unacked toward `receiver`, lowest first.
    pub fn unacked(&self, receiver: &R) -> Vec<u64> {
        self.unacked.get(receiver).map(|s| s.iter().copied().collect()).unwrap_or_default()
    }
}

/// What the receiver's duplicate window says about an arriving frame.
#[derive(Debug, PartialEq, Eq)]
pub enum Arrival<'a, S> {
    /// Never seen: the caller records it with [`RecvWindow::insert`].
    New,
    /// Seen and still held: its state.
    Held(&'a mut S),
    /// Below the sender's watermark: seen, finished and forgotten. The sender promised never to resend
    /// it, so this is a repeat of something long acknowledged.
    BelowWatermark,
}

/// One sending birth's duplicate window at a receiver: the state of every frame it sent that the
/// receiver still holds, forgotten only below the sender's `retire_below`, never by time.
#[derive(Debug)]
pub struct RecvWindow<S> {
    below: u64,
    held: BTreeMap<u64, S>,
}

impl<S> Default for RecvWindow<S> {
    fn default() -> Self {
        Self { below: 0, held: BTreeMap::new() }
    }
}

impl<S> RecvWindow<S> {
    /// An empty window.
    pub fn new() -> Self {
        Self::default()
    }

    /// A frame `seq` arrived carrying the sender's `retire_below`: forget everything below the
    /// watermark, then say what the frame is.
    pub fn arrive(&mut self, seq: u64, retire_below: u64) -> Arrival<'_, S> {
        if retire_below > self.below {
            self.below = retire_below;
            self.held = self.held.split_off(&retire_below);
        }
        if seq < self.below {
            return Arrival::BelowWatermark;
        }
        match self.held.get_mut(&seq) {
            Some(s) => Arrival::Held(s),
            None => Arrival::New,
        }
    }

    /// Record `seq` with its state.
    pub fn insert(&mut self, seq: u64, state: S) {
        self.held.insert(seq, state);
    }

    /// Forget `seq` (a request that was never dispatched): its sender's next resend is a first
    /// arrival.
    pub fn remove(&mut self, seq: u64) -> Option<S> {
        self.held.remove(&seq)
    }

    /// The state of `seq`, when held.
    pub fn get_mut(&mut self, seq: u64) -> Option<&mut S> {
        self.held.get_mut(&seq)
    }

    /// The watermark: everything below it is forgotten.
    pub fn below(&self) -> u64 {
        self.below
    }

    /// How many frames the window holds.
    pub fn len(&self) -> usize {
        self.held.len()
    }

    /// Whether the window holds nothing.
    pub fn is_empty(&self) -> bool {
        self.held.is_empty()
    }
}

/// Why a response frame breaks its invocation's order: `424 PROTOCOL_VIOLATION`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderViolation {
    /// Two different frames at one `(invocation_id, seq)`.
    Conflict {
        /// The position.
        seq: u32,
    },
    /// More than [`MAX_REORDER_POSITIONS`] past the next expected position.
    TooFarAhead {
        /// The position.
        seq: u32,
        /// The next expected position.
        next: u32,
    },
    /// A frame after the last position, `u32::MAX`, was delivered.
    PastLastPosition,
}

impl std::fmt::Display for OrderViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Conflict { seq } => write!(f, "two different response frames at position {seq}"),
            Self::TooFarAhead { seq, next } => write!(f, "response frame at position {seq} is more than {MAX_REORDER_POSITIONS} past the next expected {next}"),
            Self::PastLastPosition => write!(f, "a response frame after position {}", u32::MAX),
        }
    }
}

/// What an admitted response frame does.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Admitted {
    /// The frames now deliverable in order, starting at the one admitted: `(seq, delivery_seq, bytes)`.
    Deliver(Vec<(u32, u64, Vec<u8>)>),
    /// Held until the positions before it arrive.
    Held,
    /// The same frame again (same position, same identity): nothing new to deliver.
    Duplicate,
}

/// The order of one invocation's response frames at the caller: delivered in `seq` order, a frame
/// past a missing position held within [`MAX_REORDER_POSITIONS`], a repeat recognised by its
/// identity. Its bytes are bounded by the positions and the op's reply ceiling, which the reader
/// enforces on every frame before it is admitted here.
#[derive(Debug, Default)]
pub struct ResponseOrder {
    next: u32,
    exhausted: bool,
    delivered: Vec<u64>,
    held: BTreeMap<u32, (u64, Vec<u8>)>,
}

impl ResponseOrder {
    /// An invocation's order before its first frame.
    pub fn new() -> Self {
        Self::default()
    }

    /// The next position to deliver.
    pub fn next(&self) -> u32 {
        self.next
    }

    /// Admit the frame at `seq`, whose delivery identity's sequence is `delivery_seq`.
    pub fn admit(&mut self, seq: u32, delivery_seq: u64, bytes: Vec<u8>) -> Result<Admitted, OrderViolation> {
        if let Some(&had) = self.delivered.get(seq as usize) {
            return if had == delivery_seq { Ok(Admitted::Duplicate) } else { Err(OrderViolation::Conflict { seq }) };
        }
        if self.exhausted {
            return Err(OrderViolation::PastLastPosition);
        }
        if let Some((had, _)) = self.held.get(&seq) {
            return if *had == delivery_seq { Ok(Admitted::Duplicate) } else { Err(OrderViolation::Conflict { seq }) };
        }
        if seq != self.next {
            if seq - self.next > MAX_REORDER_POSITIONS {
                return Err(OrderViolation::TooFarAhead { seq, next: self.next });
            }
            self.held.insert(seq, (delivery_seq, bytes));
            return Ok(Admitted::Held);
        }
        let mut out = vec![(seq, delivery_seq, bytes)];
        self.deliver(delivery_seq);
        while !self.exhausted {
            let Some((d, b)) = self.held.remove(&self.next) else { break };
            out.push((self.next, d, b));
            self.deliver(d);
        }
        Ok(Admitted::Deliver(out))
    }

    fn deliver(&mut self, delivery_seq: u64) {
        self.delivered.push(delivery_seq);
        match self.next.checked_add(1) {
            Some(n) => self.next = n,
            None => self.exhausted = true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Cell 8: the resend schedule is the publish cadence, then doubling with jitter, capped at the
    /// staleness floor.
    #[test]
    fn the_resend_schedule_starts_at_the_publish_cadence_doubles_with_jitter_and_stops_at_the_floor() {
        let (cadence, floor) = (Duration::from_secs(2), Duration::from_secs(30));
        let plain: Vec<u64> = (1..=6).map(|a| resend_after(a, cadence, floor, 0.0).as_millis() as u64).collect();
        assert_eq!(plain, [2_000, 4_000, 8_000, 16_000, 30_000, 30_000], "cadence, doubling, the floor");
        assert_eq!(resend_after(1, cadence, floor, 0.25), Duration::from_millis(2_500), "+25 % jitter");
        assert_eq!(resend_after(1, cadence, floor, -0.25), Duration::from_millis(1_500), "-25 % jitter");
        assert_eq!(resend_after(1, cadence, floor, 9.0), Duration::from_millis(2_500), "jitter is clamped to 25 %");
        assert_eq!(resend_after(4, cadence, floor, 0.25), Duration::from_secs(20));
        assert_eq!(resend_after(5, cadence, floor, 0.25), floor, "never past the floor, jitter included");
        assert_eq!(resend_after(200, cadence, floor, 0.0), floor, "no overflow however long the message is owed");
    }

    /// Cell 21 (the sender's half): a new birth's counter starts at 1; one counter for every receiver.
    #[test]
    fn a_new_birth_mints_from_one_and_carries_its_lowest_unacked() {
        let mut w: SendWindow<&str> = SendWindow::new();
        assert_eq!(w.lowest_unacked(&"b"), 1, "nothing minted: the watermark is the first sequence");
        assert_eq!(w.mint(&["b"]), 1);
        assert_eq!(w.mint(&["c"]), 2);
        assert_eq!(w.mint(&["p", "b"]), 3, "a carried call is held toward its carrier and its final target");
        assert_eq!((w.lowest_unacked(&"b"), w.lowest_unacked(&"c"), w.lowest_unacked(&"p")), (1, 2, 3));
        assert!(w.settle(1));
        assert_eq!(w.lowest_unacked(&"b"), 3, "the carried frame still holds the final target's watermark");
        assert!(w.settle(3));
        assert_eq!((w.lowest_unacked(&"b"), w.lowest_unacked(&"p")), (4, 4), "nothing unacked: the next to be minted");
        assert!(!w.settle(3), "settling twice holds nothing");
    }

    /// Cell 16 (the receiver's half): duplicate state is forgotten only below the sender's watermark.
    #[test]
    fn duplicate_state_is_forgotten_only_below_the_senders_watermark() {
        let mut w: RecvWindow<&str> = RecvWindow::new();
        assert_eq!(w.arrive(5, 5), Arrival::New);
        w.insert(5, "accepted");
        assert_eq!(w.arrive(5, 5), Arrival::Held(&mut "accepted"), "a resend finds its state, however long after");
        assert_eq!(w.arrive(6, 5), Arrival::New);
        w.insert(6, "pending");
        assert_eq!(w.arrive(7, 6), Arrival::New, "the watermark passed 5");
        assert_eq!(w.len(), 1, "5 forgotten, 6 held");
        assert_eq!(w.arrive(5, 6), Arrival::BelowWatermark, "below the watermark: long acknowledged");
        assert_eq!(w.arrive(6, 3), Arrival::Held(&mut "pending"), "a lower watermark never brings anything back");
        assert_eq!(w.below(), 6);
    }

    /// Cell 14: a terminal is held while an earlier position is missing.
    #[test]
    fn a_frame_past_a_missing_position_is_held_and_delivered_in_order() {
        let mut o = ResponseOrder::new();
        assert_eq!(o.admit(2, 12, b"terminal".to_vec()), Ok(Admitted::Held), "position 1 is missing");
        assert_eq!(o.admit(0, 10, b"started".to_vec()), Ok(Admitted::Deliver(vec![(0, 10, b"started".to_vec())])));
        assert_eq!(
            o.admit(1, 11, b"data".to_vec()),
            Ok(Admitted::Deliver(vec![(1, 11, b"data".to_vec()), (2, 12, b"terminal".to_vec())])),
            "the held terminal follows the position it waited for"
        );
        assert_eq!(o.admit(1, 11, b"data".to_vec()), Ok(Admitted::Duplicate), "a resend of a delivered frame");
        assert_eq!(o.next(), 3);
    }

    /// Cell 13: two different frames at one position are a violation, whatever their identities.
    #[test]
    fn two_different_frames_at_one_position_are_a_violation() {
        let mut o = ResponseOrder::new();
        o.admit(0, 10, b"a".to_vec()).unwrap();
        assert_eq!(o.admit(0, 99, b"a".to_vec()), Err(OrderViolation::Conflict { seq: 0 }), "delivered position, another frame");
        assert_eq!(o.admit(3, 13, b"x".to_vec()), Ok(Admitted::Held));
        assert_eq!(o.admit(3, 14, b"y".to_vec()), Err(OrderViolation::Conflict { seq: 3 }), "held position, another frame");
        assert_eq!(o.admit(3, 13, b"x".to_vec()), Ok(Admitted::Duplicate));
    }

    /// Cell 20 (positions): reordering past 64 positions is refused.
    #[test]
    fn reordering_past_64_positions_is_refused() {
        let mut o = ResponseOrder::new();
        assert_eq!(o.admit(64, 1, vec![0; 4]), Ok(Admitted::Held), "64 past the next is the bound");
        assert_eq!(o.admit(65, 2, vec![0; 1]), Err(OrderViolation::TooFarAhead { seq: 65, next: 0 }));
        let mut o = ResponseOrder::new();
        for seq in 1..=64 {
            assert_eq!(o.admit(seq, u64::from(seq), vec![0; 4]), Ok(Admitted::Held));
        }
        assert!(matches!(o.admit(0, 100, vec![]), Ok(Admitted::Deliver(d)) if d.len() == 65));
    }
}
