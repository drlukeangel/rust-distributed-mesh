//! The crate's one integration-test executable: every file in this directory is a module
//! here, so the crate links once. A stem runs alone as `<exe> <stem>::`.

mod common;
mod container_pipeline;
mod i143_acceptance_2775;
mod join_before_ready;
mod pipeline_rerun;
mod probe_endpoint_close;
mod probe_root_span;
mod process_pipeline;
mod process_pipeline_failure;
mod ready_prerequisites;
mod replace_pipeline;
mod retire_admission;
mod retire_pipeline;
mod status_probe_apply;
