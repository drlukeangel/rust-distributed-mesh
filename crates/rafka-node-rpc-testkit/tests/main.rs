//! The crate's one integration-test executable: every file in this directory is a module
//! here, so the crate links once. A stem runs alone as `<exe> <stem>::`.

mod common;
mod container_pipeline;
mod hydrate_before_ready;
mod hydrate_before_ready_admin;
mod i143_acceptance_2775;
mod join_before_ready;
mod member_cert;
mod pipeline_rerun;
mod probe_endpoint_close;
mod probe_root_span;
mod process_pipeline;
mod process_pipeline_failure;
mod rafka_time_handle;
mod ready_prerequisites;
mod replace_pipeline;
mod retire_admission;
mod retire_pipeline;
mod status_probe_apply;
