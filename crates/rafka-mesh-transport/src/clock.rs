//! The clock the mesh substrate takes through its process composition.
//!
//! Every gossip timestamp (`emitted_at_rafka_ms`, `published_at_rafka_ms`, `event_at_rafka_ms`,
//! `changed_at_rafka_ms`) is Rafka-time. The substrate invents no wall clock of its own: the
//! binary that composes a [`Membership`](crate::membership::Membership) supplies the clock, and
//! every stamp on the gossip path reads it. Every RDM process composes one [`RafkaTime`]: the
//! authority's time, adopted once at its join (or its own OS clock, once, by the fabric's Day-0
//! root), then read as that reference plus monotonic elapsed time. Rafka-time says when a fact or
//! publication happened and never decides liveness, order or `Gone`: silence is the receiver's
//! own `Instant`, heartbeat order is `digest_seq`.

use std::sync::Arc;

/// A source of Rafka-time.
///
/// CONTRACT: `now_rafka_ms` is milliseconds of Rafka-time; successive reads may repeat or step
/// back, and nothing in the mesh depends on them not doing so.
pub trait Clock: Send + Sync + std::fmt::Debug {
    /// Milliseconds of Rafka-time now.
    fn now_rafka_ms(&self) -> u64;
}

/// The OS clock. It is read once, by the Day-0 root, to seed [`RafkaTime`]; no other stamp or
/// judgement reads it.
#[derive(Debug, Clone, Copy, Default)]
pub struct OsClock;

/// Milliseconds added to every [`OsClock`] read of this process. Present only under the
/// `testkit-skew` feature, which only the testkit enables: a testkit executable sets it from its
/// launch environment to prove no stamp reads the OS clock. A product build has no such symbol.
#[cfg(feature = "testkit-skew")]
static OS_CLOCK_SKEW_MS: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);

/// Skew every [`OsClock`] read of this process by `ms`. Called only by a testkit executable that
/// reads a test knob from its environment; no product build contains it.
#[cfg(feature = "testkit-skew")]
pub fn skew_os_clock_for_testkit(ms: i64) {
    OS_CLOCK_SKEW_MS.store(ms, std::sync::atomic::Ordering::Relaxed);
}

impl Clock for OsClock {
    fn now_rafka_ms(&self) -> u64 {
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0);
        #[cfg(feature = "testkit-skew")]
        let now = now.saturating_add(OS_CLOCK_SKEW_MS.load(std::sync::atomic::Ordering::Relaxed));
        now.max(0) as u64
    }
}

/// Rafka-time: node-admin's time, the one timestamp every ordering decision reads.
///
/// This is the app's way to read rafka-time. The handle is cheap to clone and every clone reads the
/// same instance: the one the process's gossip stamps, lifecycle events and fleet judgements read,
/// never a second clock. `RunningNode::rafka_time` (node) and `Running::rafka_time` (node-admin)
/// hand it to the embedding app once the process has joined, so it is always adopted by then.
///
/// A process adopts the authority's time once per join ([`RafkaTime::adopt`]); neither side
/// estimates network delay. A read answers `max(reference + monotonic elapsed, last returned)`, so
/// an adoption of a reference behind the last reading stalls the reader until the new reference
/// catches up and never steps rafka-time back. The OS clock takes no part in a read.
#[derive(Clone, Default)]
pub struct RafkaTime {
    inner: Arc<std::sync::Mutex<TimeState>>,
}

#[derive(Default)]
struct TimeState {
    /// The adopted reference and the monotonic instant it was adopted at.
    anchor: Option<(u64, std::time::Instant)>,
    /// The last value returned: reads never go below it.
    last: u64,
}

/// What one [`RafkaTime::adopt`] changed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Adopted {
    /// The reference adopted.
    pub reference_ms: u64,
    /// The reading before the adoption, when the process held rafka-time.
    pub previous_ms: Option<u64>,
    /// How far the new reference stands behind the last reading: the reader holds at that
    /// reading for this long. Zero when the reference is at or ahead of it.
    pub stalls_ms: u64,
}

impl std::fmt::Debug for RafkaTime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RafkaTime({:?})", self.try_now_ms())
    }
}

impl RafkaTime {
    /// A reader that holds no rafka-time yet: it judges nothing until a join adopts one.
    pub fn unadopted() -> Self {
        Self::default()
    }

    /// Adopt `reference_ms` as the authority's rafka-time now. The reader continues from it by
    /// monotonic elapsed time.
    pub fn adopt(&self, reference_ms: u64) -> Adopted {
        self.adopt_at(reference_ms, std::time::Instant::now())
    }

    fn adopt_at(&self, reference_ms: u64, at: std::time::Instant) -> Adopted {
        let mut s = self.inner.lock().unwrap();
        let previous_ms = s.anchor.map(|_| s.last.max(Self::read(&s, at)));
        if let Some(p) = previous_ms {
            s.last = p;
        }
        s.anchor = Some((reference_ms, at));
        Adopted { reference_ms, previous_ms, stalls_ms: previous_ms.map_or(0, |p| p.saturating_sub(reference_ms)) }
    }

    fn read(s: &TimeState, at: std::time::Instant) -> u64 {
        match s.anchor {
            Some((reference, since)) => reference.saturating_add(at.saturating_duration_since(since).as_millis() as u64).max(s.last),
            None => s.last,
        }
    }

    /// Milliseconds of rafka-time now, or `None` while the process holds none.
    pub fn try_now_ms(&self) -> Option<u64> {
        let mut s = self.inner.lock().unwrap();
        s.anchor?;
        let now = Self::read(&s, std::time::Instant::now());
        s.last = now;
        Some(now)
    }

    /// Whether the process has adopted a rafka-time.
    pub fn is_adopted(&self) -> bool {
        self.inner.lock().unwrap().anchor.is_some()
    }

    /// Milliseconds of rafka-time now. A process returns from its start only after it has adopted
    /// rafka-time, so an app holding a handle from a running node or node-admin always reads a
    /// value; a handle that holds none panics rather than answer from any other clock.
    pub fn now_ms(&self) -> u64 {
        self.try_now_ms().expect("rafka-time was read before this process adopted one from its authority")
    }
}

impl Clock for RafkaTime {
    fn now_rafka_ms(&self) -> u64 {
        self.now_ms()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    #[test]
    fn a_reader_that_adopted_nothing_answers_none() {
        let t = RafkaTime::unadopted();
        assert_eq!(t.try_now_ms(), None);
        assert!(!t.is_adopted());
    }

    #[test]
    fn a_read_is_the_reference_plus_monotonic_elapsed_time() {
        let t = RafkaTime::unadopted();
        let at = Instant::now();
        t.adopt_at(1_000_000, at);
        let s = t.inner.lock().unwrap();
        assert_eq!(RafkaTime::read(&s, at + Duration::from_millis(250)), 1_000_250);
    }

    #[test]
    fn reads_never_go_below_the_last_value_returned() {
        let t = RafkaTime::unadopted();
        t.adopt(5_000);
        let a = t.now_ms();
        let b = t.now_ms();
        assert!(b >= a);
    }

    #[test]
    fn a_re_anchor_behind_the_last_value_stalls_and_never_steps_back() {
        let t = RafkaTime::unadopted();
        let t0 = Instant::now();
        t.adopt_at(10_000, t0);
        // A reading 400 ms in: 10_400.
        {
            let mut s = t.inner.lock().unwrap();
            s.last = RafkaTime::read(&s, t0 + Duration::from_millis(400));
            assert_eq!(s.last, 10_400);
        }
        // The authority says 10_100: behind the last reading by 300 ms.
        let adopted = t.adopt_at(10_100, t0 + Duration::from_millis(400));
        assert_eq!(adopted.previous_ms, Some(10_400));
        assert_eq!(adopted.stalls_ms, 300);
        let s = t.inner.lock().unwrap();
        for (elapsed, want) in [(0, 10_400), (100, 10_400), (299, 10_400), (300, 10_400), (301, 10_401), (500, 10_600)] {
            assert_eq!(RafkaTime::read(&s, t0 + Duration::from_millis(400 + elapsed)), want, "{elapsed} ms after the re-anchor");
        }
    }

    #[test]
    fn a_re_anchor_ahead_of_the_last_value_jumps_forward() {
        let t = RafkaTime::unadopted();
        let t0 = Instant::now();
        t.adopt_at(10_000, t0);
        let adopted = t.adopt_at(20_000, t0 + Duration::from_millis(100));
        assert_eq!(adopted.stalls_ms, 0);
        let s = t.inner.lock().unwrap();
        assert_eq!(RafkaTime::read(&s, t0 + Duration::from_millis(100)), 20_000);
    }

    #[test]
    fn every_clone_reads_the_one_instance() {
        let a = RafkaTime::unadopted();
        let b = a.clone();
        a.adopt(7_000_000);
        assert!(b.is_adopted());
        assert!(b.now_ms() >= 7_000_000);
    }
}

/// The clock a process composes, shared by everything it publishes.
pub type SharedClock = Arc<dyn Clock>;

/// The OS clock, shared.
pub fn os_clock() -> SharedClock {
    Arc::new(OsClock)
}
