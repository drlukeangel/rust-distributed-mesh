//! Deployment: the runtime hand (PRD §8; mesh-control-plane.md §5–§6).
//!
//! Node-admin owns topology, identity, endpoints, readiness and restart vs
//! replacement; a provider only realises or retires one runtime.

pub mod endpoint;
pub mod provider;
