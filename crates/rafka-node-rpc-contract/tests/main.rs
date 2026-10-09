//! The crate's one integration-test executable: every file in this directory is a module
//! here, so the crate links once. A stem runs alone as `<exe> <stem>::`.

mod build_claim_wire;
mod build_facts_wire;
mod forward_wire;
mod join_wire;
mod status_wire;
mod topology_wire;
