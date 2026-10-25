//! Node tickle primitive (`docs/architecture/lifecycles/node-tickle.md`).
//!
//! The one reach ladder over one exact birth: one probe direct, and if that fails the same probe
//! through up to two live peers of the node's mesh, sent at once; `reached` with what the node said,
//! or `unreached`, never dead. Run by node recovery, mesh investigation, the seat watch and the
//! successor ladder of node.primary.update, each over its own [`TickleCalls`].

pub use crate::offline::{birth_answered, eligible_carriers, tickle, tickle_ladder, tickle_ladder_at_default_budget, PeerProbe, ProbeEnd, Tickle, TickleCalls, TickleClient, Unreached, VIA_PEER_TICKLE_FANOUT};
