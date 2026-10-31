//! The one absolute deadline a provider call runs under (Q304, rdm#241; deployment-provider.md §2).
//!
//! The deployment layer owns the policy: the pipeline takes a step's deadline from the step's bound,
//! the requested stop grace and any earlier caller deadline, and passes that one absolute instant
//! down through [`DeploymentProvider`](super::provider::DeploymentProvider). Every provider call in
//! the step spends the same budget; nesting never extends it ([`Deadline::within`]). A provider
//! enforces the deadline it is handed around its external I/O and chooses no timeout of its own: an
//! expiry ends the client wait and is a named failure,
//! [`DeployError::Deadline`](super::provider::DeployError::Deadline), which proves neither the
//! cancellation of an external mutation nor the runtime's exit.

use std::time::Duration;
use tokio::time::Instant;

/// A step's budget for its provider calls, before a graceful stop's grace is added: the bound every
/// ordinary provider command answers within (rdm#241 §4).
pub const STEP_BOUND: Duration = Duration::from_secs(30);

/// How long a client killed at its deadline is waited for before the failure is returned; the reap
/// stays owned after that (rdm#241 §4).
pub const CLEANUP_BOUND: Duration = Duration::from_secs(2);

/// One absolute deadline and the bound it was taken from (the bound is what a span names).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Deadline {
    at: Instant,
    bound: Duration,
}

impl Deadline {
    /// The deadline `bound` from now.
    pub fn after(bound: Duration) -> Self {
        Self { at: Instant::now() + bound, bound }
    }

    /// A step's deadline: [`STEP_BOUND`] from now.
    pub fn step() -> Self {
        Self::after(STEP_BOUND)
    }

    /// A graceful stop's deadline: its requested grace and [`STEP_BOUND`] from now, so the grace the
    /// stop asks for is never cut short by the client bound.
    pub fn stop(grace: Duration) -> Self {
        Self::after(grace + STEP_BOUND)
    }

    /// The earlier of this deadline and `caller`'s: a caller's shorter budget wins, and nothing
    /// nested in it ever runs past it.
    pub fn within(self, caller: Option<Deadline>) -> Self {
        match caller {
            Some(c) if c.at < self.at => c,
            _ => self,
        }
    }

    /// The instant the deadline falls at.
    pub fn at(&self) -> Instant {
        self.at
    }

    /// The bound the deadline was taken from.
    pub fn bound(&self) -> Duration {
        self.bound
    }

    /// What is left of it now; zero once it has passed.
    pub fn remaining(&self) -> Duration {
        self.at.saturating_duration_since(Instant::now())
    }

    /// `work` until the deadline: `Err` with the time elapsed when it passes first. `work` is
    /// dropped at the deadline, so what it owns is ended with it.
    pub async fn run<T>(&self, work: impl std::future::Future<Output = T>) -> Result<T, Duration> {
        let started = Instant::now();
        tokio::time::timeout_at(self.at, work).await.map_err(|_| started.elapsed())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_callers_shorter_deadline_wins_and_a_longer_one_never_extends() {
        let step = Deadline::step();
        let short = Deadline::after(Duration::from_millis(50));
        assert_eq!(step.within(Some(short)), short, "the caller's shorter budget wins");
        let long = Deadline::after(Duration::from_secs(600));
        assert_eq!(step.within(Some(long)), step, "a longer caller budget never extends the step's");
        assert_eq!(step.within(None), step);
    }

    #[tokio::test]
    async fn a_graceful_stop_keeps_its_grace_inside_the_deadline() {
        let grace = Duration::from_secs(40);
        let d = Deadline::stop(grace);
        assert_eq!(d.bound(), grace + STEP_BOUND);
        assert!(d.remaining() > grace, "the stop's grace is never cut short by the client bound");
    }

    #[tokio::test]
    async fn work_past_the_deadline_ends_with_the_time_it_took() {
        let d = Deadline::after(Duration::from_millis(30));
        let elapsed = d.run(std::future::pending::<()>()).await.expect_err("pending work ends at the deadline");
        assert!(elapsed >= Duration::from_millis(25), "{elapsed:?}");
        assert_eq!(Deadline::after(Duration::from_secs(5)).run(async { 7 }).await, Ok(7));
    }
}
