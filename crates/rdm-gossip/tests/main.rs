//! rdm-gossip's cells: the frames, the topology view, the channels and the application channels.

#[allow(dead_code)]
#[path = "../../../tools/test-support/own_process.rs"]
mod own_process;

#[ctor::ctor]
fn process_env() {
    if std::env::var_os("RDM_TEST_OWN_PROCESS").is_some() || std::env::var_os("RDM_OUTAGE_CHILD").is_some() {
        return;
    }
    let dir = std::env::var_os("I143_ACCEPTANCE_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").join("target/evidence/rdm-gossip"));
    std::fs::create_dir_all(&dir).expect("the evidence directory can be created");
    std::env::set_var("RDM_EVIDENCE_DIR", dir);
}

pub(crate) fn evidence_dir() -> std::path::PathBuf {
    std::path::PathBuf::from(std::env::var_os("RDM_EVIDENCE_DIR").expect("main.rs sets RDM_EVIDENCE_DIR before any test runs"))
}

#[allow(dead_code)]
#[path = "../../../tools/test-support/gossip_wire.rs"]
mod gossip_wire;

mod nodes_aggregate;
mod status_reinforcement;
mod refeed_scheduler;
mod held_member_repair;
mod channel_stopped;
mod peer_location_view;
mod gossip_stats;
mod load_sampling;
mod heartbeat_spans;
mod lifecycle_frames_wire;
mod digest_frames_wire;
mod primary_frames_wire;
mod publication_wake;
mod forward_role;
mod no_acting_publication;
mod peer_mesh_deletion;
mod deleted_mesh_aggregate;
mod stopped_birth;
mod served_topology;
mod publisher_handover;
mod deleted_status;
mod app_channels;
