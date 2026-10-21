//! Node recovery workflow (`docs/architecture/lifecycles/node-recovery.md`).
//!
//! Reconciles an unheard or failed node: detecting silence, running the one tickle ladder
//! ([`crate::workflows::node_tickle`]), proving runtime exit, and initiating recovery attempts.

pub use crate::build_run::{run_opened_recovery, run_recovery, Recovered, RecoveryNotRun};
pub use crate::offline::{tickle_round, tickle_round_at_default_budget, VIA_PEER_TICKLE_FANOUT};
pub use crate::admin::{durable_births, recovery_due, watched_births, ExitVerdicts, Inspected};
