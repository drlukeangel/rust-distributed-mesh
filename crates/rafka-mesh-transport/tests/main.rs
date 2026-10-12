//! The crate's one integration-test executable: every file in this directory is a module
//! here, so the crate links once. A stem runs alone as `<exe> <stem>::`.

#[path = "../../../tools/test-support/own_process.rs"]
mod own_process;
mod i143_acceptance_2900;
mod i143_acceptance_2901;
mod i143_acceptance_2902;
mod channel_park;
mod peer_location_book;
mod gossip_stats;
mod heartbeat_spans;
mod lifecycle_frames_wire;
mod digest_frames_wire;
mod seat_frames_wire;
mod round_frames_wire;
mod round_wake;
mod forward_role;
mod no_acting_publication;
mod stopped_birth;
mod publisher_handover;
mod departed_status;
