//! Mesh investigation workflow (`docs/architecture/lifecycles/mesh-investigation.md`).
//!
//! The fabric-primary's investigation of an unheard peer mesh: probing via peer carriers,
//! evaluating reachability ladders, and deciding mesh recovery.

/// Peer-mesh investigation ladder, rungs, probe outcomes, and steps (`docs/architecture/lifecycles/mesh-investigation.md`).
pub use crate::investigate::*;
