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

    /// A stamp the process's authority published, offered as evidence of the authority's
    /// rafka-time. The caller has already proved the sample eligible (the authority's exact birth,
    /// freshly admitted, ready-for-traffic); this decides what the clock does with it. A clock that
    /// is not disciplined from heartbeats, such as [`OsClock`], ignores every sample.
    fn observe(&self, _stamp_ms: u64) -> Observed {
        Observed::Ignored(Ignored::NotDisciplined)
    }
}

/// The jitter allowance, in milliseconds: how far behind its local clock a sample may stand and
/// still be ordinary delivery delay. Provisional.
pub const JITTER_ALLOWANCE_MS: u64 = 500;
/// The samples of one decision window.
pub const WINDOW_SAMPLES: usize = 8;
/// A sample older than this (local monotonic time) no longer counts toward a window.
pub const WINDOW_SPAN: std::time::Duration = std::time::Duration::from_secs(120);
/// The most a clock's advancement is reduced while it sheds an excess, in parts per million.
pub const MAX_SLEW_PPM: u32 = 500;

/// What one [`Clock::observe`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Observed {
    /// The stamp was ahead of the clock: its reference moved forward to the stamp.
    Stepped {
        /// How far ahead of the clock the stamp stood, in milliseconds.
        by_ms: u64,
    },
    /// A full window of samples all stood behind the clock by more than the jitter allowance: the
    /// clock sheds the excess at the maximum slew rate.
    SlewStarted {
        /// The best (least negative) offset of the window, in milliseconds.
        best_offset_ms: i64,
        /// What is shed: how far past the jitter allowance the best sample stood.
        excess_ms: u64,
        /// The rate reduction applied, in parts per million.
        ppm: u32,
        /// How long the shed takes at that rate, in milliseconds.
        recovers_in_ms: u64,
    },
    /// A full window stood within the jitter allowance (or its best sample was not behind by more
    /// than it): the clock is left as it is.
    Within {
        /// The best offset of the window, in milliseconds.
        best_offset_ms: i64,
        /// The window's peak-to-peak offset spread (the network jitter), in milliseconds.
        jitter_ms: u64,
    },
    /// The sample was kept; the window is not yet full.
    Collecting,
    /// The sample moved nothing.
    Ignored(Ignored),
}

/// Why a sample moved nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ignored {
    /// The clock takes no samples.
    NotDisciplined,
    /// The process holds no rafka-time yet: a heartbeat never anchors a clock, only an adoption
    /// from the authority's answer does.
    NotAdopted,
    /// The stamp is zero: the publisher synthesized the digest.
    NoStamp,
    /// The stamp is not newer than the newest one taken: a duplicate or a reordered delivery.
    NotNewer,
}

impl Ignored {
    /// The reason as it appears in a span.
    pub fn reason(self) -> &'static str {
        match self {
            Self::NotDisciplined => "clock-not-disciplined",
            Self::NotAdopted => "no-rafka-time-adopted",
            Self::NoStamp => "zero-stamp",
            Self::NotNewer => "stamp-not-newer",
        }
    }
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

    /// [`Clock::observe`] at `at`.
    pub fn observe_at(&self, _stamp_ms: u64, _at: std::time::Instant) -> Observed {
        Observed::Ignored(Ignored::NotDisciplined)
    }

    /// The reading at `at` (a monotonic instant not before any earlier one given), `None` while
    /// the process holds none.
    pub fn reading_at(&self, at: std::time::Instant) -> Option<u64> {
        let mut s = self.inner.lock().unwrap();
        s.anchor?;
        let now = Self::read(&s, at);
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

    fn observe(&self, stamp_ms: u64) -> Observed {
        self.observe_at(stamp_ms, std::time::Instant::now())
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

#[cfg(test)]
mod discipline_sim {
    //! Deterministic simulation of the discipline: a follower whose monotonic clock runs at
    //! `1 + ppm/1e6` of true time hears an authority's heartbeats, each stamped with the
    //! authority's rafka-time when emitted and delivered after a delay. No wall clock, no sleeps.
    use super::*;
    use std::time::{Duration, Instant};

    const T0: u64 = 1_800_000_000_000;

    struct Sim {
        base: Instant,
        ppm: i64,
        t: RafkaTime,
    }

    impl Sim {
        /// A follower that adopted `T0 + offset_ms` at true time zero.
        fn new(ppm: i64, offset_ms: i64) -> Self {
            let base = Instant::now();
            let t = RafkaTime::unadopted();
            t.adopt_at((T0 as i64 + offset_ms) as u64, base);
            Self { base, ppm, t }
        }

        /// The follower's monotonic instant at true time `true_us`.
        fn at(&self, true_us: u64) -> Instant {
            self.base + Duration::from_nanos(((true_us as i128) * 1000 * (1_000_000 + self.ppm as i128) / 1_000_000) as u64)
        }

        fn auth(true_ms: u64) -> u64 {
            T0 + true_ms
        }

        /// The follower's reading less the authority's rafka-time at true time `true_ms`.
        fn err_ms(&self, true_ms: u64) -> i64 {
            self.t.reading_at(self.at(true_ms * 1000)).unwrap() as i64 - Self::auth(true_ms) as i64
        }

        /// Run true time from `from_ms` to `to_ms`: the authority emits every `every_ms` (when
        /// `delay(k)` is `Some(d)` the heartbeat `k` arrives `d` ms after emission, else it is
        /// lost); the follower is read every `read_ms`. `on_read(true_ms, err_ms)` sees each read
        /// and `on_obs(true_ms, outcome)` each observation. Returns the outcomes in order.
        fn run(&self, from_ms: u64, to_ms: u64, every_ms: u64, delay: &dyn Fn(u64) -> Option<u64>, read_ms: u64, on_read: &mut dyn FnMut(u64, i64), on_obs: &mut dyn FnMut(u64, Observed)) {
            let mut deliveries: Vec<(u64, u64)> = Vec::new(); // (deliver_ms, emit_ms)
            let first_k = from_ms.div_ceil(every_ms);
            let mut k = first_k;
            while k * every_ms < to_ms {
                let e = k * every_ms;
                if let Some(d) = delay(k) {
                    deliveries.push((e + d, e));
                }
                k += 1;
            }
            deliveries.sort();
            let mut di = deliveries.iter().filter(|(d, _)| *d >= from_ms && *d <= to_ms).peekable();
            let mut next_read = from_ms.div_ceil(read_ms) * read_ms;
            let mut last_reading = 0u64;
            loop {
                let nd = di.peek().map(|(d, _)| *d);
                match (nd, next_read <= to_ms) {
                    (Some(d), r) if !r || d <= next_read => {
                        let (_, e) = di.next().unwrap();
                        let o = self.t.observe_at(Self::auth(*e), self.at(d * 1000));
                        on_obs(d, o);
                    }
                    (_, true) => {
                        let r = self.t.reading_at(self.at(next_read * 1000)).unwrap();
                        assert!(r >= last_reading, "reads never step back: {r} after {last_reading} at {next_read} ms");
                        last_reading = r;
                        on_read(next_read, r as i64 - Self::auth(next_read) as i64);
                        next_read += read_ms;
                    }
                    _ => break,
                }
            }
        }
    }

    fn quiet(_: u64, _: Observed) {}
    const H: u64 = 3_600_000;

    #[test]
    fn a_sample_before_any_anchor_moves_nothing() {
        let t = RafkaTime::unadopted();
        assert_eq!(t.observe_at(T0, Instant::now()), Observed::Ignored(Ignored::NotAdopted));
        assert!(!t.is_adopted());
        assert_eq!(t.try_now_ms(), None);
    }

    #[test]
    fn the_os_clock_ignores_every_sample() {
        assert_eq!(OsClock.observe(T0), Observed::Ignored(Ignored::NotDisciplined));
    }

    #[test]
    fn a_slow_clock_steps_forward_and_stays_within_the_delivery_delay_of_the_authority() {
        // 50 ppm slow, heartbeats every 2 s delivered after 20 ms, three hours.
        let sim = Sim::new(-50, 0);
        let (mut lo, mut hi, mut steps) = (i64::MAX, i64::MIN, 0);
        sim.run(0, 3 * H, 2_000, &|_| Some(20), 1_000, &mut |_, e| {
            lo = lo.min(e);
            hi = hi.max(e);
        }, &mut |_, o| steps += matches!(o, Observed::Stepped { .. }) as u32);
        assert!(lo >= -20 - 1 - 1, "never behind by more than the delay plus the drift of one gap and a tick: {lo}");
        assert!(hi <= 0, "never ahead of the authority: {hi}");
        assert!(steps > 0, "a slow clock is stepped");
    }

    #[test]
    fn a_clock_behind_by_seconds_converges_in_one_heartbeat() {
        let sim = Sim::new(0, -4_000);
        let mut first_ok = None;
        sim.run(0, 60_000, 2_000, &|_| Some(10), 100, &mut |t, e| {
            if first_ok.is_none() && e >= -10 {
                first_ok = Some(t);
            }
        }, &mut quiet);
        // The first heartbeat is emitted at 0 and arrives at 10 ms.
        assert!(first_ok.unwrap() <= 110, "converged by {:?} ms", first_ok);
    }

    #[test]
    fn a_fast_clock_is_held_within_the_jitter_allowance_of_the_authority() {
        // 50 ppm fast for six hours: unchecked it would stand 1.08 s ahead.
        let sim = Sim::new(50, 0);
        let (mut hi, mut slews, mut max_ppm) = (i64::MIN, 0, 0u32);
        sim.run(0, 6 * H, 2_000, &|_| Some(20), 1_000, &mut |_, e| hi = hi.max(e), &mut |_, o| {
            if let Observed::SlewStarted { ppm, .. } = o {
                slews += 1;
                max_ppm = max_ppm.max(ppm);
            }
        });
        let allowance = JITTER_ALLOWANCE_MS as i64;
        assert!(slews > 0, "a clock that stands ahead of the allowance is slewed");
        assert!(max_ppm <= MAX_SLEW_PPM);
        assert!(hi <= allowance + 5, "ahead by at most the allowance plus one window of drift: {hi}");
    }

    #[test]
    fn a_clock_two_seconds_ahead_is_slewed_back_without_ever_stepping_back() {
        let sim = Sim::new(0, 2_000);
        let (mut prev, mut dips) = (i64::MAX, 0);
        let (mut slewing_slope_ok, mut samples) = (true, Vec::new());
        sim.run(0, 4_000_000, 2_000, &|_| Some(20), 10_000, &mut |t, e| {
            samples.push((t, e));
            if e > prev {
                dips += 1;
            }
            prev = e;
        }, &mut quiet);
        // The error never grows, and it stands within the allowance once the excess is shed:
        // 2000 + 20 - 500 = 1520 ms at 500 ppm takes 3040 s.
        assert_eq!(dips, 0, "the error shrinks monotonically");
        let at = |t: u64| samples.iter().find(|(x, _)| *x >= t).unwrap().1;
        assert!(at(3_200_000) <= JITTER_ALLOWANCE_MS as i64, "recovered by 3200 s: {}", at(3_200_000));
        assert!(at(3_200_000) >= JITTER_ALLOWANCE_MS as i64 - 60, "and not past the allowance: {}", at(3_200_000));
        // While shedding, the clock advances at 1 - 500e-6 of true time.
        let (a, b) = (at(100_000), at(200_000));
        let rate_ppm = (a - b) as f64 / 100_000.0 * 1e6;
        slewing_slope_ok &= (rate_ppm - MAX_SLEW_PPM as f64).abs() < 20.0;
        assert!(slewing_slope_ok, "shed at the maximum rate, measured {rate_ppm} ppm");
    }

    #[test]
    fn samples_behind_within_the_allowance_move_nothing() {
        // An exact clock, delays up to 400 ms (all inside the allowance), 30 minutes.
        let sim = Sim::new(0, 0);
        let control = RafkaTime::unadopted();
        control.adopt_at(T0, sim.base);
        let mut acted = 0;
        sim.run(0, 1_800_000, 2_000, &|k| Some((k * 7919) % 400), 1_000, &mut |t, _| {
            let c = control.reading_at(sim.at(t * 1000)).unwrap();
            let m = sim.t.reading_at(sim.at(t * 1000)).unwrap();
            assert_eq!(m, c, "the disciplined reading equals the untouched one at {t} ms");
        }, &mut |_, o| acted += matches!(o, Observed::Stepped { .. } | Observed::SlewStarted { .. }) as u32);
        assert_eq!(acted, 0);
    }

    #[test]
    fn one_delayed_heartbeat_among_prompt_ones_slews_nothing() {
        let sim = Sim::new(0, 0);
        let mut acted = 0;
        // Every eighth heartbeat takes 3 s; the rest take 20 ms.
        sim.run(0, 600_000, 2_000, &|k| Some(if k % 8 == 0 { 3_000 } else { 20 }), 1_000, &mut |_, _| {}, &mut |_, o| acted += matches!(o, Observed::SlewStarted { .. }) as u32);
        assert_eq!(acted, 0, "the window's best sample is the prompt one");
    }

    #[test]
    fn a_constant_delay_past_the_allowance_sheds_once_and_goes_no_further() {
        // Every heartbeat takes 2 s: indistinguishable from a clock 1.5 s past the allowance.
        // The shed is bounded by the excess, so the clock rests (2000 - 500) ms behind and stays.
        let sim = Sim::new(0, 0);
        let mut last_err = 0;
        sim.run(0, 8_000_000, 2_000, &|_| Some(2_000), 10_000, &mut |_, e| last_err = e, &mut quiet);
        let want = -(2_000 - JITTER_ALLOWANCE_MS as i64);
        assert!((last_err - want).abs() <= 30, "rests at {want} ms, got {last_err}");
    }

    #[test]
    fn the_authority_changing_retargets_to_the_new_holder() {
        // The new holder's clock stands 300 ms ahead of the old one's: the follower steps to it.
        let sim = Sim::new(0, 0);
        sim.run(0, 60_000, 2_000, &|_| Some(10), 1_000, &mut |_, _| {}, &mut quiet);
        let mut stepped = None;
        for k in 31..40u64 {
            let e = k * 2_000;
            let o = sim.t.observe_at(Sim::auth(e) + 300, sim.at((e + 10) * 1000));
            if let Observed::Stepped { by_ms } = o {
                stepped.get_or_insert(by_ms);
            }
        }
        let by = stepped.expect("stepped to the new holder");
        assert!((285..=300).contains(&by), "{by}");
        let e = sim.err_ms(80_010);
        assert!((e - 290).abs() <= 15, "{e}");
    }

    #[test]
    fn a_partition_and_a_rejoin_leave_the_clock_continuous_and_re_converge() {
        // 50 ppm slow; heartbeats lost for 20 minutes; then heard again.
        let sim = Sim::new(-50, 0);
        let mut max_gap_err = 0;
        let mut after = None;
        sim.run(0, 3_000_000, 2_000, &|k| (!(100..700).contains(&k)).then_some(20), 1_000, &mut |t, e| {
            if (200_000..1_400_000).contains(&t) {
                max_gap_err = max_gap_err.min(e);
            }
            if t == 1_405_000 {
                after = Some(e);
            }
        }, &mut quiet);
        // 20 minutes at -50 ppm: 60 ms behind at most.
        assert!(max_gap_err >= -70, "{max_gap_err}");
        assert!(after.unwrap() >= -21 - 1, "re-converged: {:?}", after);
    }

    #[test]
    fn the_clock_advances_right_after_an_ahead_sample() {
        let sim = Sim::new(0, -1_000);
        let o = sim.t.observe_at(Sim::auth(100), sim.at(110_000));
        assert!(matches!(o, Observed::Stepped { .. }), "{o:?}");
        let r0 = sim.t.reading_at(sim.at(110_000)).unwrap();
        let r1 = sim.t.reading_at(sim.at(110_000 + 250_000)).unwrap();
        assert_eq!(r0, Sim::auth(100));
        assert_eq!(r1 - r0, 250, "advances in step with elapsed time at once");
    }

    #[test]
    fn a_duplicate_or_reordered_stamp_is_ignored() {
        let sim = Sim::new(0, 0);
        assert!(!matches!(sim.t.observe_at(Sim::auth(2_000), sim.at(2_010_000)), Observed::Ignored(_)));
        assert_eq!(sim.t.observe_at(Sim::auth(2_000), sim.at(2_020_000)), Observed::Ignored(Ignored::NotNewer));
        assert_eq!(sim.t.observe_at(Sim::auth(1_000), sim.at(2_030_000)), Observed::Ignored(Ignored::NotNewer));
        assert_eq!(sim.t.observe_at(0, sim.at(2_040_000)), Observed::Ignored(Ignored::NoStamp));
    }

    #[test]
    fn concurrent_observes_and_reads_never_step_back() {
        let t = RafkaTime::unadopted();
        // An observation before any anchor races with the adoption.
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let observers: Vec<_> = (0..4u64)
            .map(|i| {
                let (t, stop) = (t.clone(), stop.clone());
                std::thread::spawn(move || {
                    let mut stamp = T0 + i;
                    while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                        stamp += 7;
                        t.observe(stamp);
                    }
                })
            })
            .collect();
        let readers: Vec<_> = (0..4)
            .map(|_| {
                let (t, stop) = (t.clone(), stop.clone());
                std::thread::spawn(move || {
                    let mut last = 0;
                    let mut reads = 0u64;
                    while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                        if let Some(now) = t.try_now_ms() {
                            assert!(now >= last, "a read stepped back: {now} after {last}");
                            last = now;
                            reads += 1;
                        }
                    }
                    reads
                })
            })
            .collect();
        std::thread::sleep(Duration::from_millis(50));
        t.adopt(T0);
        std::thread::sleep(Duration::from_millis(300));
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        for o in observers {
            o.join().unwrap();
        }
        let reads: u64 = readers.into_iter().map(|r| r.join().unwrap()).sum();
        assert!(reads > 0);
    }
}
