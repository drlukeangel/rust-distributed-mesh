//! The crate's one integration-test executable: every file in this directory is a module
//! here, so the crate links once. A stem runs alone as `<exe> <stem>::`.

#[path = "../../../tools/test-support/own_process.rs"]
mod own_process;
mod i143_acceptance_2900;
mod i143_acceptance_2901;
mod i143_acceptance_2902;
mod gossip_stats;
mod heartbeat_spans;
mod lifecycle_frames_wire;
mod seat_frames_wire;
