//! Generic node-admin core for the Mesh product (i143 PRD §4, §7).
//!
//! Owns the generic Fabric → Mesh → Node model and its topology invariants,
//! and the local process table that used to live inside the Admin UI. Build,
//! the state adapter, deployment providers, lifecycle transitions and
//! elections land here in later stories; nothing here knows a Rafka role.

pub mod build;
pub mod build_state;
pub mod deployment;
pub mod http;
pub mod model;
pub mod process_table;
pub mod topology;
