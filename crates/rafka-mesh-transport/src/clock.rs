//! The clock the mesh substrate takes through its process composition.
//!
//! Every gossip timestamp (`emitted_at_rafka_ms`, `published_at_rafka_ms`, `event_at_rafka_ms`,
//! `changed_at_rafka_ms`) is Rafka-time. The substrate invents no wall clock of its own: the
//! binary that composes a [`Membership`](crate::membership::Membership) supplies the clock, and
//! every stamp on the gossip path reads it. RDM's binaries supply [`OsClock`]; a product that
//! has adopted a Rafka-time source supplies that source. Rafka-time says when a fact or
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

/// The OS clock as a Rafka-time source: what an RDM binary supplies.
#[derive(Debug, Clone, Copy, Default)]
pub struct OsClock;

impl Clock for OsClock {
    fn now_rafka_ms(&self) -> u64 {
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
    }
}

/// The clock a process composes, shared by everything it publishes.
pub type SharedClock = Arc<dyn Clock>;

/// The OS clock, shared.
pub fn os_clock() -> SharedClock {
    Arc::new(OsClock)
}
