//! Generic node-admin core for the Mesh product (i143 PRD §4, §7).
//!
//! Owns the generic Fabric → Mesh → Node model and its topology invariants,
//! Build (intents, planner, state adapters, the fabric projection, the
//! executor), the control routes, deployment providers and pipelines, and
//! lifecycle transitions. It is the only lifecycle authority: the Admin UI
//! and every other front end are clients of its control API
//! (`rafka-node-admin-client`). Nothing here knows a Rafka role.

pub mod build;
pub mod build_state;
pub mod deployment;
pub mod executor;
pub mod fabric_builds;
pub mod http;
pub mod lifecycle;
pub mod model;
pub mod readiness;
pub mod topology;
