//! The gate a stopped node-admin's background tasks stop at.
//!
//! A node-admin that is stopped keeps its process, its endpoint and everything it holds, and does
//! nothing outward until it is started again. Its tasks are held two ways:
//! - a task that takes the view lock, or any other lock a handler of the stopped admin needs, begins
//!   each turn at its own wait point ([`StopGate::turn`]), where it holds nothing, and ends the turn
//!   (drops its [`Turn`]) before its idle wait. It is never held mid-critical-section, so the stopped
//!   admin always serves its start (node-start: start is the only exit from stopped Leaving), and
//!   each turn waits before it sends. A turn already past its wait point when the gate stops runs to
//!   the end of that turn: a stop holds the gate with [`StopGate::quiesce`], which resolves once every such
//!   turn has ended, so nothing a turn does follows the stop's cut. Work a turn starts on a task of
//!   its own and waits on begins its own turn under it ([`Turn::under`]), never at the wait point.
//!   The same rule the other way checklist: a stop of this node that a turn commands and waits on does
//!   not wait for that turn. A Build's drive pass and its run tag their turns with the Build
//!   ([`Turn::of_build`]); a stop this node's own run commands (fabric-delete.md step 4: the
//!   fabric-primary's own node.delete, which it executes) holds with
//!   [`StopGate::quiesce_commanded_by`], which waits for every turn but that Build's: those turns
//!   wait on the stop, and what they do after it is the stop's caller's own work. Every other stop
//!   (one another node commands, a signal's) waits for every turn;
//! - a task that takes no such lock is spawned through the gate ([`StopGate::spawn`]): while the
//!   gate is stopped its future is not polled, so it holds its place at the await it was at.
//! Work a stop deletes with the birth instead of holding it runs as [`StopGate::until_stopped`]: it
//! ends at the stop and never resumes. The gate restarts nothing.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::watch;

/// Where the gate stands.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GateState {
    /// No turn begins while the gate is stopped.
    pub stopped: bool,
    /// The turns past their wait point and not yet ended, by the task that runs them.
    pub turns: BTreeMap<&'static str, usize>,
    /// Those of `turns` that are a Build's (its drive pass, its run), by Build id, then task.
    pub builds: BTreeMap<String, BTreeMap<&'static str, usize>>,
}

impl GateState {
    /// The turns in progress, as `task` or `task:n`, comma-separated; empty when there are none.
    pub fn named(&self) -> String {
        self.turns.iter().map(|(t, n)| if *n == 1 { t.to_string() } else { format!("{t}:{n}") }).collect::<Vec<_>>().join(",")
    }

    /// How many turns are in progress.
    pub fn in_progress(&self) -> usize {
        self.turns.values().sum()
    }

    /// The gate without the turns of Build `build`: what a stop that Build's own run commands
    /// waits for.
    pub fn but_build(&self, build: &str) -> GateState {
        let mut left = self.clone();
        if let Some(spared) = left.builds.remove(build) {
            for (task, n) in spared {
                if let Some(m) = left.turns.get_mut(task) {
                    *m = m.saturating_sub(n);
                    if *m == 0 {
                        left.turns.remove(task);
                    }
                }
            }
        }
        left
    }
}

/// One turn of `task` fewer in `turns`.
fn less(turns: &mut BTreeMap<&'static str, usize>, task: &'static str) {
    if let Some(n) = turns.get_mut(task) {
        *n -= 1;
        if *n == 0 {
            turns.remove(task);
        }
    }
}

/// One turn of `task` fewer among Build `build`'s.
fn untag(s: &mut GateState, build: &str, task: &'static str) {
    if let Some(of) = s.builds.get_mut(build) {
        less(of, task);
        if of.is_empty() {
            s.builds.remove(build);
        }
    }
}

/// A stop's quiesce begins to wait for the turns `at` names: said on its own span when the wait
/// begins, so a wait that never ends still names the turns it waits for.
fn waits_for(at: &GateState) {
    if at.in_progress() > 0 {
        tracing::info_span!("rdm.node_admin.node.update.via-stop-quiesce-waiting", turns_in_progress = at.in_progress(), waiting_for = %at.named())
            .in_scope(|| tracing::info!("the stop waits for the turns in progress at it to end"));
    }
}

/// One gate, shared by every task of a node-admin.
#[derive(Clone)]
pub struct StopGate {
    state: Arc<watch::Sender<GateState>>,
}

/// One turn of a gated task, from its wait point to its idle wait: it ends when dropped.
#[must_use = "a turn ends when it is dropped"]
pub struct Turn {
    state: Arc<watch::Sender<GateState>>,
    task: &'static str,
    /// The Build this turn is of ([`Turn::of_build`]).
    build: Option<String>,
}

impl Turn {
    /// A turn of `task` begun under this one, at once, whether the gate is stopped or not: for work
    /// this turn starts on a task of its own and waits on (a drive pass's own run of the attempt it
    /// dispatched), which would otherwise wait at the gate for the turn that waits on it. This turn
    /// was past its wait point, so the stop's quiesce has not resolved; it waits for the new turn as
    /// for every other, until the work drops it. It is of this turn's Build, when this turn is one's.
    pub fn under(&self, task: &'static str) -> Turn {
        let build = self.build.clone();
        self.state.send_modify(|s| {
            *s.turns.entry(task).or_default() += 1;
            if let Some(b) = &build {
                *s.builds.entry(b.clone()).or_default().entry(task).or_default() += 1;
            }
        });
        Turn { state: self.state.clone(), task, build }
    }

    /// This turn as one of Build `build` (its id): a stop that Build's own run commands and waits on
    /// does not wait for it ([`StopGate::quiesce_commanded_by`]).
    pub fn of_build(mut self, build: &str) -> Turn {
        let task = self.task;
        let was = self.build.replace(build.to_string());
        self.state.send_modify(|s| {
            if let Some(b) = was.as_deref() {
                untag(s, b, task);
            }
            *s.builds.entry(build.to_string()).or_default().entry(task).or_default() += 1;
        });
        self
    }
}

impl Drop for Turn {
    fn drop(&mut self) {
        let (task, build) = (self.task, self.build.take());
        self.state.send_modify(|s| {
            less(&mut s.turns, task);
            if let Some(b) = build.as_deref() {
                untag(s, b, task);
            }
        });
    }
}

impl Default for StopGate {
    fn default() -> Self {
        Self::new()
    }
}

impl StopGate {
    /// An open gate.
    pub fn new() -> Self {
        Self { state: Arc::new(watch::Sender::new(GateState::default())) }
    }

    /// Stop polling every task spawned through this gate, and begin no turn. Answers the gate as it
    /// stood at that moment: each turn in progress runs to its end.
    pub fn stop(&self) -> GateState {
        let mut at = GateState::default();
        self.state.send_modify(|s| {
            s.stopped = true;
            at = s.clone();
        });
        at
    }

    /// Stop, and resolve once no turn is in progress: the turns already past their wait point have
    /// ended, on their own completion, and no other begins. Answers the gate as it stood at the stop.
    /// A turn holds no lock at its wait point, and none is held here, so every turn can end.
    pub async fn quiesce(&self) -> GateState {
        let at = self.stop();
        waits_for(&at);
        let mut rx = self.state.subscribe();
        let _ = rx.wait_for(|s| s.turns.is_empty()).await;
        at
    }

    /// [`Self::quiesce`] for a stop of this node that Build `build`'s own run commands and waits on:
    /// it resolves once every turn but that Build's has ended. Those turns (the run, and the drive pass
    /// it runs under) wait on this stop, so it never waits for them; what they do after it is the
    /// stop's caller's own remaining work. Answers the gate as it stood at the stop, without them.
    pub async fn quiesce_commanded_by(&self, build: &str) -> GateState {
        let at = self.stop().but_build(build);
        waits_for(&at);
        let mut rx = self.state.subscribe();
        let _ = rx.wait_for(|s| s.but_build(build).turns.is_empty()).await;
        at
    }

    /// Poll them again, and let turns begin.
    pub fn resume(&self) {
        self.state.send_modify(|s| s.stopped = false);
    }

    /// Whether the gate is stopped.
    pub fn is_stopped(&self) -> bool {
        self.state.borrow().stopped
    }

    /// The gate's state, as it moves.
    pub fn state(&self) -> watch::Receiver<GateState> {
        self.state.subscribe()
    }

    /// A turn of `task` begun at once, whether the gate is stopped or not: for work that ends at the stop
    /// ([`Self::until_stopped`]) and whatever it writes after it ends (its span, its outcome), so a stop's
    /// quiesce resolves only once that work has ended, never while it is still ending.
    pub fn begun(&self, task: &'static str) -> Turn {
        self.state.send_modify(|s| *s.turns.entry(task).or_default() += 1);
        Turn { state: self.state.clone(), task, build: None }
    }

    /// Begin a turn of `task`: at once while the gate is open; while it is stopped, when it opens. A
    /// task that takes a lock other code needs is not spawned behind the gate (it would be held
    /// wherever it stands, a lock it was queued on included): it begins each turn here, at a point
    /// where it holds nothing, and drops the [`Turn`] before its idle wait. Whether the gate is open
    /// and the turn's count are read and moved as one, so no turn begins on a gate that stopped.
    pub async fn turn(&self, task: &'static str) -> Turn {
        let mut rx = self.state.subscribe();
        loop {
            let _ = rx.wait_for(|s| !s.stopped).await;
            let begun = self.state.send_if_modified(|s| {
                if s.stopped {
                    return false;
                }
                *s.turns.entry(task).or_default() += 1;
                true
            });
            if begun {
                return Turn { state: self.state.clone(), task, build: None };
            }
        }
    }

    /// Spawn `task` behind this gate: held wherever it stands while the gate is stopped. Only for a
    /// task that takes no lock another task or handler needs (a task held while waiting on, or
    /// holding, such a lock keeps it from them); every other task begins its turns at
    /// [`StopGate::turn`].
    pub fn spawn<F>(&self, task: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        tokio::spawn(self.hold(task))
    }

    /// `task`, held wherever it stands while the gate is stopped: what [`StopGate::spawn`] runs, for a
    /// caller that runs it on a task of its own (a `JoinSet`). The same rule: only for a task that
    /// takes no lock another task or handler needs.
    pub fn hold<F>(&self, task: F) -> impl Future<Output = F::Output> + Send + 'static
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let mut rx = self.state.subscribe();
        async move {
            tokio::pin!(task);
            loop {
                if rx.borrow().stopped && rx.wait_for(|s| !s.stopped).await.is_err() {
                    return std::future::pending().await;
                }
                // The stop is looked at first: a task woken by its own work and by the stop at once is
                // held where it stands, never polled one step past the stop.
                tokio::select! {
                    biased;
                    _ = rx.wait_for(|s| s.stopped) => {}
                    out = &mut task => return out,
                }
            }
        }
    }

    /// `work`, ended the moment the gate stops: `None` when the stop came first (or the gate was
    /// already stopped). For work a stop deletes with the birth instead of holding it (a tickle ladder, a
    /// recovery follower): it is dropped where it stands, a lock it held or was queued on included, and
    /// never resumes; the next turn after a start decides again. It is a turn while it runs, so a stop's
    /// quiesce resolves only once it has ended. The stop is looked at first, so work
    /// woken by its own progress and by the stop at once is never polled past the stop.
    pub fn until_stopped<F>(&self, work: F) -> impl Future<Output = Option<F::Output>> + Send + 'static
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let mut rx = self.state.subscribe();
        let gate = self.clone();
        async move {
            // A turn for the work's whole life: a stop's quiesce resolves once it has ended.
            let _turn = gate.begun("until-stopped");
            tokio::select! {
                biased;
                _ = rx.wait_for(|s| s.stopped) => None,
                out = work => Some(out),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    /// CONTRACT (node-stop: a stop holds its tasks before it cuts): a quiesce stops the gate at once
    /// and resolves only once every turn that was in progress at the stop has ended, on its own
    /// completion; it answers those turns by task. No turn begins after the stop until the gate
    /// opens. Must NOT happen: a quiesce that resolves while a turn is still running, or a turn
    /// begun on a stopped gate.
    #[tokio::test]
    async fn a_quiesce_resolves_only_once_every_turn_in_progress_has_ended_and_none_begins_after_it() {
        use futures_util::FutureExt;
        let gate = StopGate::new();
        let a = gate.turn("hierarchy").await;
        let b = gate.turn("executor").await;
        let quiesce = gate.quiesce();
        tokio::pin!(quiesce);
        assert!((&mut quiesce).now_or_never().is_none(), "two turns are in progress: the quiesce waits");
        assert!(gate.is_stopped(), "the quiesce stopped the gate at once");
        let late = gate.turn("declarer");
        tokio::pin!(late);
        assert!((&mut late).now_or_never().is_none(), "no turn begins on a stopped gate");
        drop(a);
        assert!((&mut quiesce).now_or_never().is_none(), "one turn is still in progress: the quiesce waits");
        drop(b);
        let at = (&mut quiesce).now_or_never().expect("every turn ended: the quiesce resolves");
        assert_eq!((at.in_progress(), at.named().as_str()), (2, "executor,hierarchy"), "it answers the turns it waited for");
        assert!((&mut late).now_or_never().is_none(), "still stopped: the late turn has not begun");
        gate.resume();
        let late = (&mut late).now_or_never().expect("the gate opened: the turn begins");
        assert_eq!(gate.state().borrow().named(), "declarer");
        drop(late);
        assert_eq!(gate.state().borrow().in_progress(), 0);
    }

    /// CONTRACT (fabric-delete.md step 4: the fabric-primary executes its own node.delete, whose stop
    /// holds its tasks while the run that commanded it waits on it; stop_gate.rs: a stop a turn
    /// commands and waits on does not wait for that turn): a drive pass of Build b1 and the run it
    /// dispatched under it, a run of Build b2 and a declarer turn are in progress. A quiesce commanded
    /// by b1's run stops the gate at once, waits for the b2 run and the declarer, and resolves with
    /// b1's pass and run still in progress, answering only the turns it waited for; those end later
    /// on their own. A plain quiesce on the same turns waits for b1's as well. Must NOT happen: the
    /// stop waiting on the run that waits on it, or a quiesce commanded by b1 that skips another
    /// Build's turn.
    #[tokio::test]
    async fn a_quiesce_commanded_by_a_builds_own_run_waits_for_every_turn_but_that_builds() {
        use futures_util::FutureExt;
        let gate = StopGate::new();
        let pass = gate.turn("build-drive").await.of_build("b1");
        let run = pass.under("build-run");
        let other = gate.turn("build-run").await.of_build("b2");
        let declarer = gate.turn("declarer").await;
        let quiesce = gate.quiesce_commanded_by("b1");
        tokio::pin!(quiesce);
        assert!((&mut quiesce).now_or_never().is_none(), "b2's run and the declarer are in progress: the quiesce waits");
        assert!(gate.is_stopped(), "the quiesce stopped the gate at once");
        drop(other);
        assert!((&mut quiesce).now_or_never().is_none(), "the declarer is still in progress: the quiesce waits");
        drop(declarer);
        let at = (&mut quiesce).now_or_never().expect("every turn but b1's ended: the quiesce resolves while b1's run waits on it");
        assert_eq!((at.in_progress(), at.named().as_str()), (2, "build-run,declarer"), "it answers the turns it waited for, not b1's");
        assert_eq!(gate.state().borrow().named(), "build-drive,build-run", "b1's pass and run are still in progress");
        let plain = gate.quiesce();
        tokio::pin!(plain);
        assert!((&mut plain).now_or_never().is_none(), "a stop no run of b1 commanded waits for b1's turns");
        drop(run);
        drop(pass);
        assert!((&mut plain).now_or_never().is_some(), "b1's turns ended: the plain quiesce resolves");
        let s = gate.state().borrow().clone();
        assert!(s.turns.is_empty() && s.builds.is_empty(), "every count is back to nothing: {s:?}");
    }

    /// CONTRACT (stop_gate.rs: a task spawned through the gate is not polled while it is stopped): a
    /// task woken by its own work and by the stop at the same moment is held where it stands; it
    /// makes no step past the stop. Each of the 64 trials gives the gate one fresh such moment (a
    /// select that polled its branches in a random order would step past the stop about half the
    /// time). Must NOT happen: a stopped task making one more step of its own work.
    #[tokio::test]
    async fn a_task_woken_by_its_own_work_and_the_stop_at_once_makes_no_step_past_the_stop() {
        for trial in 0..64 {
            let gate = StopGate::new();
            let (work, steps) = (Arc::new(tokio::sync::Notify::new()), Arc::new(AtomicU64::new(0)));
            let (w, n) = (work.clone(), steps.clone());
            let task = gate.spawn(async move {
                loop {
                    w.notified().await;
                    n.fetch_add(1, Ordering::SeqCst);
                }
            });
            // The task polls once and waits on its work; then its work and the stop come together.
            tokio::task::yield_now().await;
            work.notify_one();
            gate.stop();
            tokio::time::sleep(Duration::from_millis(5)).await;
            assert_eq!(steps.load(Ordering::SeqCst), 0, "trial {trial}: the stopped task made a step past the stop");
            task.abort();
        }
    }

    /// CONTRACT: a task behind a stopped gate makes no progress and keeps its place; the same task
    /// resumes where it was when the gate opens. What must NOT happen: a stopped task that keeps
    /// running, or one that starts over. On the paused clock: the tick at 30 ms and the stop at 45 ms fire
    /// in deadline order, never as one timer turn whose wakes the scheduler orders.
    #[tokio::test(start_paused = true)]
    async fn a_task_held_at_the_gate_holds_its_place_and_resumes_from_it() {
        let gate = StopGate::new();
        let ticks = Arc::new(AtomicU64::new(0));
        let t = ticks.clone();
        let done = gate.spawn(async move {
            for _ in 0..3 {
                tokio::time::sleep(Duration::from_millis(30)).await;
                t.fetch_add(1, Ordering::SeqCst);
            }
        });
        tokio::time::sleep(Duration::from_millis(45)).await;
        gate.stop();
        tokio::time::sleep(Duration::from_millis(20)).await;
        let held = ticks.load(Ordering::SeqCst);
        assert_eq!(held, 1, "one tick before the stop");
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(ticks.load(Ordering::SeqCst), held, "no progress while stopped");
        gate.resume();
        done.await.unwrap();
        assert_eq!(ticks.load(Ordering::SeqCst), 3, "it finished the rest, not from the start");
    }
}
