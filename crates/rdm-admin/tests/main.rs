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

/// The binary's process environment, set once, here, before `main` and before any thread exists
/// (one binary is one process): a cell never sets or removes a variable. The gate's cadence
/// (`tools/test-runner/src/lib.rs` CADENCE), each value only where the environment the binary was
/// started with names none: the gate's own export, or the environment a delegating parent hands the
/// child it starts for one cell (`own_process::delegated_with_env`, a cell whose process needs other
/// values: a stretched round, its own evidence directory, a runtime socket), stays as given.
#[ctor::ctor]
fn process_env() {
    for (k, v) in [("RDM_STALENESS_MS", "3000"), ("RDM_GOSSIP_INTERVAL_MS", "500"), ("RDM_BACKBONE_INTERVAL_MS", "500")] {
        if std::env::var_os(k).is_none() {
            std::env::set_var(k, v);
        }
    }
}

/// The rafka-time a test admin holds, adopted from a reference nowhere near any OS clock.
pub fn adopted_time() -> rdm_transport::clock::RafkaTime {
    let t = rdm_transport::clock::RafkaTime::unadopted();
    t.adopt(1_000_000);
    t
}

mod admin_serves_forward;
mod ids_crockford;
mod backbone_anti_entropy;
mod boot_stop;
mod loopback;
mod mesh_delete_resume;
mod mesh_delete_round;
mod mesh_delete_source;
mod mesh_delete_view;
mod build_catch_up;
mod build_admission;
mod build_claim;
mod build_drive;
mod build_run;
mod build_facts_read;
mod build_takeover;
mod command_admission_status;
mod connections_writer;
mod container_proof;
mod container_unsupported;
mod current_build;
mod drift_convergence;
mod fabric_handover;
mod fabric_not_ready;
mod status_rpc;
mod executor_redecide;
mod connections_reconnect;
mod gossip_frames_wire;
mod abandoned_join;
mod join_admission;
mod join_wire;
mod node_objects;
mod node_rows;
mod node_rpc_objects;
mod stop_gate_quiesce;
mod stop_gate_view_lock;
mod process_launch_environment;
mod provider_policy;
mod readiness_gates;
mod receipt_refusal;
mod recovery_admission;
mod restart_hold_from_facts;
mod restart_one_operation;
mod pipeline_moves_no_seat;
mod pipeline_step_io;
mod status_declarations;
mod status_seat_fold;
mod stopped_delete;
mod stopped_gc;
mod stopped_view;
mod store_root_boot;
mod topology_read;
mod fabric_delete;
