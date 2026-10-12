//! The crate's one integration-test executable: every file in this directory is a module
//! here, so the crate links once. A stem runs alone as `<exe> <stem>::`.

#[path = "../../../tools/test-support/own_process.rs"]
mod own_process;

mod common;
mod container_pipeline;
mod hydrate_before_ready;
mod hydrate_before_ready_admin;
mod i143_acceptance_2775;
mod join_before_ready;
mod member_cert;
mod member_cert_admin;
mod pipeline_rerun;
mod probe_endpoint_close;
mod probe_root_span;
mod process_pipeline;
mod process_pipeline_failure;
mod rafka_time_handle;
mod ready_prerequisites;
mod recover_pipeline;
mod replace_pipeline;
mod retire_admission;
mod retire_pipeline;
mod status_probe_apply;
mod park_start;
mod gossip_shutdown_once;
mod mesh_leave_member;
mod rig_teardown;
mod apply_node_name;
mod rejoin_name;
mod command_natural_key;
mod transport_diag;
mod connections_rename;
mod rename_precondition;
mod connections_delete;
mod no_caller_check;
mod node_reads;
mod node_start_sequence;
mod admin_top_up;
mod admin_start_declares;
