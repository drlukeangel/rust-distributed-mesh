//! Provider-neutral scenario, evidence and replay contracts for the generic
//! Mesh product (i143 PRD §6, §17, §20).
//!
//! Every run drives a live estate only through public surfaces: the
//! node-admin control API (`docs/i143/design.md` §4), the `rafka-rpc-probe`
//! binary (§5) and the evidence files (§6). Nothing here calls a handler
//! in-process.

pub mod estate;
pub mod runner;
pub mod scenario;
