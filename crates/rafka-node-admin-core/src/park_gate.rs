//! The gate a parked node-admin's background tasks stop at.
//!
//! A node-admin that is stopped keeps its process, its endpoint and everything it holds, and does
//! nothing outward until it is started again. Its tasks are held two ways:
//! - a task that takes the view lock, or any other lock a handler of the parked admin needs, waits
//!   at its own wait point for the gate to open ([`ParkGate::opened`]), where it holds nothing. It
//!   is never held mid-critical-section, so the parked admin always serves its start (node-start:
//!   start is the only exit from stopped Leaving), and each turn waits before it sends;
//! - a task that takes no such lock is spawned through the gate ([`ParkGate::spawn`]): while the
//!   gate is parked its future is not polled, so it holds its place at the await it was at.
//! Work a stop retires with the birth instead of holding it runs as [`ParkGate::until_parked`]: it
//! ends at the park and never resumes. The gate restarts nothing.

use std::future::Future;
use std::sync::Arc;
use tokio::sync::watch;

/// One gate, shared by every task of a node-admin.
#[derive(Clone)]
pub struct ParkGate {
    parked: Arc<watch::Sender<bool>>,
}

impl Default for ParkGate {
    fn default() -> Self {
        Self::new()
    }
}

impl ParkGate {
    /// An open gate.
    pub fn new() -> Self {
        Self { parked: Arc::new(watch::Sender::new(false)) }
    }

    /// Stop polling every task spawned through this gate.
    pub fn park(&self) {
        self.parked.send_replace(true);
    }

    /// Poll them again.
    pub fn resume(&self) {
        self.parked.send_replace(false);
    }

    /// Whether the gate is parked.
    pub fn is_parked(&self) -> bool {
        *self.parked.borrow()
    }

    /// Returns at once while the gate is open; while it is parked, when it opens. A task that takes
    /// a lock other code needs is not spawned behind the gate (it would be held wherever it stands,
    /// a lock it was queued on included): it waits here, at a point where it holds nothing.
    pub async fn opened(&self) {
        let mut rx = self.parked.subscribe();
        let _ = rx.wait_for(|p| !*p).await;
    }

    /// Spawn `task` behind this gate: held wherever it stands while the gate is parked. Only for a
    /// task that takes no lock another task or handler needs (a task held while waiting on, or
    /// holding, such a lock keeps it from them); every other task waits at [`ParkGate::opened`].
    pub fn spawn<F>(&self, task: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let mut rx = self.parked.subscribe();
        tokio::spawn(async move {
            tokio::pin!(task);
            loop {
                if *rx.borrow() && rx.wait_for(|p| !*p).await.is_err() {
                    return std::future::pending().await;
                }
                tokio::select! {
                    out = &mut task => return out,
                    _ = rx.wait_for(|p| *p) => {}
                }
            }
        })
    }

    /// `work`, ended the moment the gate parks: `None` when the park came first (or the gate was
    /// already parked). For work a stop retires with the birth instead of holding it (a tickle round, a
    /// recovery follower): it is dropped where it stands, a lock it held or was queued on included, and
    /// never resumes; the next turn after a start decides again. The park is looked at first, so work
    /// woken by its own progress and by the park at once is never polled past the park.
    pub fn until_parked<F>(&self, work: F) -> impl Future<Output = Option<F::Output>> + Send + 'static
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let mut rx = self.parked.subscribe();
        async move {
            tokio::select! {
                biased;
                _ = rx.wait_for(|p| *p) => None,
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
