//! The crate's one integration-test executable: every file in this directory is a module
//! here, so the crate links once. A stem runs alone as `<exe> <stem>::`.

#[path = "../../../tools/test-support/own_process.rs"]
mod own_process;

/// Cells that capture spans install a dispatcher on their own thread. tracing decides a callsite's
/// interest from the dispatchers it can see when the callsite first registers, and with exactly one
/// dispatcher in the process it consults only the registering thread's: a callsite first reached on a
/// thread of another cell is then disabled for the capturing cell. A second dispatcher, held and never
/// installed as any thread's default, makes tracing consult every live dispatcher at registration, so a
/// capturing cell sees its callsites whichever thread reached them first. It is never a default: a
/// process-wide default would hand a worker thread a registry that has not got the span a capturing
/// cell's endpoint stored as an explicit parent, and creating the child would panic. Call before
/// installing a dispatcher.
pub fn enable_callsites() {
    static HELD: std::sync::OnceLock<tracing::Dispatch> = std::sync::OnceLock::new();
    HELD.get_or_init(|| tracing::Dispatch::new(tracing_subscriber::registry()));
}
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
mod apply_node_name;
mod rejoin_name;
mod command_natural_key;
mod transport_diag;
mod connections_rename;
