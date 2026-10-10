//! The crate's one integration-test executable: every file in this directory is a module
//! here, so the crate links once. A stem runs alone as `<exe> <stem>::`.

mod catalog_seam;
mod certainty;
mod common;
#[allow(dead_code)]
#[path = "common/forward_rig.rs"]
mod rig;
mod connection_observer;
mod connections_integration;
mod context;
mod endpoint_bind_span_stack;
mod exited_birth;
mod flood;
mod forward;
mod forward_spans;
mod i143_acceptance_2780;
mod i143_acceptance_2781;
mod moved_birth;
mod one_endpoint;
mod pool;
mod ready_gate;
mod routing;
mod settled_replies;
mod streaming;
