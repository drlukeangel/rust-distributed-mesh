//! Node tickle primitive (`docs/architecture/lifecycles/node-tickle.md`).
//!
//! Probes whether a node is reachable: one probe direct, and if that fails, probes
//! through up to two live peers of the node's mesh sent concurrently.

pub use crate::offline::{tickle_round, tickle_round_at_default_budget, VIA_PEER_TICKLE_FANOUT};
pub use crate::seat_watch::birth_answered;
