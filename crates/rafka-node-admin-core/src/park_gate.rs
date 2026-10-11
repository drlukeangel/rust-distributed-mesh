//! The gate a parked node-admin's background tasks stop at.
//!
//! A node-admin that is stopped keeps its process, its endpoint and everything it holds, and does
//! nothing outward until it is started again. Its tasks are held two ways:
//! - a task that takes the view lock, or any other lock a handler of the parked admin needs, begins
//!   each turn at its own wait point ([`ParkGate::turn`]), where it holds nothing, and ends the turn
//!   (drops its [`Turn`]) before its idle wait. It is never held mid-critical-section, so the parked
//!   admin always serves its start (node-start: start is the only exit from stopped Leaving), and
//!   each turn waits before it sends. A turn already past its wait point when the gate parks runs to
//!   the end of that turn; [`ParkGate::park`] answers how many were;
//! - a task that takes no such lock is spawned through the gate ([`ParkGate::spawn`]): while the
//!   gate is parked its future is not polled, so it holds its place at the await it was at.
//! The gate neither cancels nor restarts a task.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::watch;

/// Where the gate stands.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GateState {
    /// No turn begins while the gate is parked.
    pub parked: bool,
    /// The turns past their wait point and not yet ended, by the task that runs them.
    pub turns: BTreeMap<&'static str, usize>,
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
}

/// One gate, shared by every task of a node-admin.
#[derive(Clone)]
pub struct ParkGate {
    state: Arc<watch::Sender<GateState>>,
}

/// One turn of a gated task, from its wait point to its idle wait: it ends when dropped.
#[must_use = "a turn ends when it is dropped"]
pub struct Turn {
    state: Arc<watch::Sender<GateState>>,
    task: &'static str,
}

impl Drop for Turn {
    fn drop(&mut self) {
        let task = self.task;
        self.state.send_modify(|s| {
            if let Some(n) = s.turns.get_mut(task) {
                *n -= 1;
                if *n == 0 {
                    s.turns.remove(task);
                }
            }
        });
    }
}

impl Default for ParkGate {
    fn default() -> Self {
        Self::new()
    }
}

impl ParkGate {
    /// An open gate.
    pub fn new() -> Self {
        Self { state: Arc::new(watch::Sender::new(GateState::default())) }
    }

    /// Stop polling every task spawned through this gate, and begin no turn. Answers the gate as it
    /// stood at that moment: each turn in progress runs to its end.
    pub fn park(&self) -> GateState {
        let mut at = GateState::default();
        self.state.send_modify(|s| {
            s.parked = true;
            at = s.clone();
        });
        at
    }

    /// Poll them again, and let turns begin.
    pub fn resume(&self) {
        self.state.send_modify(|s| s.parked = false);
    }

    /// Whether the gate is parked.
    pub fn is_parked(&self) -> bool {
        self.state.borrow().parked
    }

    /// The gate's state, as it moves.
    pub fn state(&self) -> watch::Receiver<GateState> {
        self.state.subscribe()
    }

    /// Begin a turn of `task`: at once while the gate is open; while it is parked, when it opens. A
    /// task that takes a lock other code needs is not spawned behind the gate (it would be held
    /// wherever it stands, a lock it was queued on included): it begins each turn here, at a point
    /// where it holds nothing, and drops the [`Turn`] before its idle wait. Whether the gate is open
    /// and the turn's count are read and moved as one, so no turn begins on a gate that parked.
    pub async fn turn(&self, task: &'static str) -> Turn {
        let mut rx = self.state.subscribe();
        loop {
            let _ = rx.wait_for(|s| !s.parked).await;
            let begun = self.state.send_if_modified(|s| {
                if s.parked {
                    return false;
                }
                *s.turns.entry(task).or_default() += 1;
                true
            });
            if begun {
                return Turn { state: self.state.clone(), task };
            }
        }
    }

    /// Spawn `task` behind this gate: held wherever it stands while the gate is parked. Only for a
    /// task that takes no lock another task or handler needs (a task held while waiting on, or
    /// holding, such a lock keeps it from them); every other task begins its turns at
    /// [`ParkGate::turn`].
    pub fn spawn<F>(&self, task: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let mut rx = self.state.subscribe();
        tokio::spawn(async move {
            tokio::pin!(task);
            loop {
                if rx.borrow().parked && rx.wait_for(|s| !s.parked).await.is_err() {
                    return std::future::pending().await;
                }
                tokio::select! {
                    out = &mut task => return out,
                    _ = rx.wait_for(|s| s.parked) => {}
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    /// CONTRACT: a task behind a parked gate makes no progress and keeps its place; the same task
    /// resumes where it was when the gate opens. What must NOT happen: a parked task that keeps
    /// running, or one that starts over.
    #[tokio::test]
    async fn a_parked_task_holds_its_place_and_resumes_from_it() {
        let gate = ParkGate::new();
        let ticks = Arc::new(AtomicU64::new(0));
        let t = ticks.clone();
        let done = gate.spawn(async move {
            for _ in 0..3 {
                tokio::time::sleep(Duration::from_millis(30)).await;
                t.fetch_add(1, Ordering::SeqCst);
            }
        });
        tokio::time::sleep(Duration::from_millis(45)).await;
        gate.park();
        tokio::time::sleep(Duration::from_millis(20)).await;
        let held = ticks.load(Ordering::SeqCst);
        assert_eq!(held, 1, "one tick before the park");
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(ticks.load(Ordering::SeqCst), held, "no progress while parked");
        gate.resume();
        done.await.unwrap();
        assert_eq!(ticks.load(Ordering::SeqCst), 3, "it finished the rest, not from the start");
    }
}
