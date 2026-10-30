#[allow(dead_code)]
#[path = "../../../tools/test-support/own_process.rs"]
mod own_process;

mod build_claim_wire;
mod read_wire;
mod build_wire;
mod cadence;
mod connections;
mod crockford60_parity;
mod forward_wire;
mod handover_wire;
mod join_wire;
mod node_command_wire;
mod reconnect;
mod status_wire;
mod topology_wire;
mod forward_causes;

mod outcome_codes;
mod app_types_wire;
