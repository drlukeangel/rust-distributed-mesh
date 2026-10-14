//! The crate's one integration-test executable: every file in this directory is a module
//! here, so the crate links once. A stem runs alone as `<exe> <stem>::`.

// A cell that needs its own process here needs its own environment too: the shared file's plain `delegated` is unused.
#[allow(dead_code)]
#[path = "../../../tools/test-support/own_process.rs"]
mod own_process;

/// The process environment this binary writes, once, before any thread exists: a test binary is one
/// process, so no test sets a variable. Evidence goes to the gate's `I143_ACCEPTANCE_DIR` when it
/// exports one, else to this crate's own directory under `target/evidence`.
#[ctor::ctor]
fn process_env() {
    let dir = std::env::var_os("I143_ACCEPTANCE_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").join("target/evidence/rafka-mesh-transport"));
    std::fs::create_dir_all(&dir).expect("the evidence directory can be created");
    std::env::set_var("RDM_EVIDENCE_DIR", dir);
}

/// The evidence directory the binary's start-up chose: every cell's `EvidenceCapture` writes its spans
/// here (and to the collector when `OTEL_EXPORTER_OTLP_ENDPOINT` names one), each in a file of its own,
/// so a cell reads back only the spans its own threads emitted.
pub(crate) fn evidence_dir() -> std::path::PathBuf {
    std::path::PathBuf::from(std::env::var_os("RDM_EVIDENCE_DIR").expect("main.rs sets RDM_EVIDENCE_DIR before any test runs"))
}

mod i143_acceptance_2900;
mod i143_acceptance_2901;
mod i143_acceptance_2902;
mod channel_stopped;
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
mod deleted_mesh_aggregate;
mod stopped_birth;
mod publisher_handover;
mod departed_status;
