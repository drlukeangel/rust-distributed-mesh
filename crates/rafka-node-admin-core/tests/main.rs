//! The crate's one integration-test executable: every file in this directory is a module
//! here, so the crate links once. A stem runs alone as `<exe> <stem>::`.

/// Only this story's spans reached the exporter: every one is RDM's own (`rdm.*`). A transport
/// internal (iroh, noq) or any other foreign span in a cell's trace is a defect of the capture.
pub fn assert_own_story<'a>(names: impl IntoIterator<Item = &'a str>) {
    let foreign: std::collections::BTreeSet<&str> = names.into_iter().filter(|n| !n.starts_with("rdm.")).collect();
    assert!(foreign.is_empty(), "spans outside the cell's story were exported: {foreign:?}");
}

#[path = "../../../tools/test-support/own_process.rs"]
mod own_process;

/// The rafka-time a test admin holds, adopted from a reference nowhere near any OS clock.
pub fn adopted_time() -> rafka_mesh_transport::clock::RafkaTime {
    let t = rafka_mesh_transport::clock::RafkaTime::unadopted();
    t.adopt(1_000_000);
    t
}

mod admin_serves_forward;
mod backbone_anti_entropy;
mod boot_stop;
mod loopback;
mod mesh_leave_resume;
mod mesh_delete_round;
mod mesh_delete_source;
mod build_catch_up;
mod build_admission;
mod build_claim;
mod build_drive;
mod build_facts_read;
mod build_takeover;
mod client_contract;
mod command_admission_status;
mod connections_writer;
mod container_proof;
mod container_unsupported;
mod control_routes;
mod current_build;
mod drift_convergence;
mod fabric_handover;
mod fabric_pointer_rows;
mod i143_acceptance_2805;
mod i143_acceptance_2892;
mod i143_acceptance_2938;
mod i143_acceptance_rw1;
mod i143_rb2_abandoned_join;
mod i143_rj1_join;
mod join_wire;
mod node_objects;
mod node_rows;
mod node_rpc_objects;
mod park_gate_quiesce;
mod park_gate_view_lock;
mod process_launch_environment;
mod provider_policy;
mod readiness_gates;
mod receipt_refusal;
mod recovery_admission;
mod restart_hold_from_facts;
mod restart_one_operation;
mod seat_transfer_step;
mod status_declarations;
mod status_seat_fold;
mod stopped_delete;
mod stopped_gc;
mod store_root_boot;
mod topology_read;
