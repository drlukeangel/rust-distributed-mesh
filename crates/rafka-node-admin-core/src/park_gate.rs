//! The gate a parked node-admin's background tasks stop at.
//!
//! A node-admin that is stopped keeps its process, its endpoint and everything it holds, and does
//! nothing outward until it is started again. Each task it runs is spawned through its gate: while
//! the gate is parked the task's future is not polled, so it holds its place at the await it was at
//! and resumes from there. The gate neither cancels nor restarts a task.

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

    /// Spawn `task` behind this gate.
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
