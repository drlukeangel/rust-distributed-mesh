//! The crate's one integration-test executable: every file in this directory is a module
//! here, so the crate links once. A stem runs alone as `<exe> <stem>::`.

#[allow(dead_code)]
#[path = "../../../tools/test-support/own_process.rs"]
mod own_process;

/// The process environment this binary writes, once, before any thread exists: a test binary is one
/// process, so no test sets a variable. Evidence goes to the gate's `I143_ACCEPTANCE_DIR` when it
/// exports one, else to this crate's own directory under `target/evidence`.
/// Child processes spawned via `own_process` or `collector_outage` keep the exact environment their
/// parent gave them.
#[ctor::ctor]
fn process_env() {
    if std::env::var_os("RDM_TEST_OWN_PROCESS").is_some() || std::env::var_os("RDM_OUTAGE_CHILD").is_some() {
        return;
    }
    let dir = std::env::var_os("I143_ACCEPTANCE_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").join("target/evidence/rdm-transport"));
    std::fs::create_dir_all(&dir).expect("the evidence directory can be created");
    std::env::set_var("RDM_EVIDENCE_DIR", dir);
}

/// The evidence directory the binary's start-up chose: every cell's `EvidenceCapture` writes its spans
/// here (and to the collector when `OTEL_EXPORTER_OTLP_ENDPOINT` names one), each in a file of its own,
/// so a cell reads back only the spans its own threads emitted.
pub(crate) fn evidence_dir() -> std::path::PathBuf {
    std::path::PathBuf::from(std::env::var_os("RDM_EVIDENCE_DIR").expect("main.rs sets RDM_EVIDENCE_DIR before any test runs"))
}

// gossip & topology cells
mod nodes_snapshot;
mod status_reinforcement;
mod refeed_scheduler;
mod channel_stopped;
mod peer_location_view;
mod gossip_stats;
mod heartbeat_spans;
mod lifecycle_frames_wire;
mod digest_frames_wire;
mod seat_frames_wire;
mod state_hook_frames_wire;
mod publication_wake;
mod forward_role;
mod no_acting_publication;
mod deleted_mesh_aggregate;
mod stopped_birth;
mod served_topology;
mod publisher_handover;
mod deleted_status;

// node-rpc cells
mod catalog_seam;
mod certainty;
mod common;
#[allow(dead_code)]
#[path = "common/forward_rig.rs"]
mod rig;
mod connection_cut;
mod connection_observer;
mod connections_integration;
mod context;
mod delivery;
mod endpoint_bind_span_stack;
mod exited_birth;
mod flood;
mod forward;
mod owed_route;
mod forward_spans;
mod moved_birth;
mod one_endpoint;
mod pool;
mod ready_gate;
mod routing;
mod settled_replies;
mod streaming;

// telemetry cells
mod collector_outage;
mod telemetry_init;
mod telemetry_exit;
mod evidence_capture;
mod otlp_http_export;
mod runtime_watchdog;
mod span_end_nonblocking;

#[allow(dead_code)]
#[path = "../../../tools/test-support/gossip_wire.rs"]
mod gossip_wire;

mod app_channels;
mod connections;
