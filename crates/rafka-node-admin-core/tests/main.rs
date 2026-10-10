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

/// The rafka-time a test admin holds, adopted from a reference nowhere near any OS clock.
pub fn adopted_time() -> rafka_mesh_transport::clock::RafkaTime {
    let t = rafka_mesh_transport::clock::RafkaTime::unadopted();
    t.adopt(1_000_000);
    t
}

mod admin_serves_forward;
mod build_catch_up;
mod build_claim;
mod build_facts_read;
mod build_takeover;
mod client_contract;
mod connections_writer;
mod container_proof;
mod container_unsupported;
mod control_routes;
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
mod process_launch_environment;
mod provider_policy;
mod readiness_gates;
mod status_declarations;
mod topology_read;
