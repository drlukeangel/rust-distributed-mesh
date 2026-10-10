//! The mesh gossip substrate: the membership digest book, the versioned cross-mesh topology
//! snapshots and deltas, the one postcard wire codec for gossip frames, the packing that fits
//! messages to iroh-gossip's frame limit, and the Rafka-time clock seam.
//!
//! - [`membership`]: the gossip membership, the digest book and the cut-off rule.
//! - [`snapshot`]: the state machines over `topology_version`, chunked snapshots and deltas.
//! - [`wire`]: the codec every gossip frame goes through.
//! - [`chunking`]: ordered packing into gossip messages.
//! - [`clock`]: the source of Rafka-time a binary supplies.
#![deny(missing_docs)]


pub mod chunking;
pub mod clock;
pub(crate) mod discipline;
pub mod iroh_obs;
pub mod load;
pub mod membership;
pub mod snapshot;
pub mod wire;
