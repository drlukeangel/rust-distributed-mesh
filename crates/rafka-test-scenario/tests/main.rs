//! The crate's one integration-test executable: every file in this directory is a module
//! here, so the crate links once. A stem runs alone as `<exe> <stem>::`.

#![allow(non_snake_case)]

/// Only this story's spans reached the exporter: every one is RDM's own (`rdm.*`). A transport
/// internal (iroh, noq) or any other foreign span in a cell's trace is a defect of the capture.
pub fn assert_own_story<'a>(names: impl IntoIterator<Item = &'a str>) {
    let foreign: std::collections::BTreeSet<&str> = names.into_iter().filter(|n| !n.starts_with("rdm.")).collect();
    assert!(foreign.is_empty(), "spans outside the cell's story were exported: {foreign:?}");
}

/// How long after a member's exact runtime is killed its mesh-primary's node recovery has put a
/// new node, ready, at its path.name (node-recovery.md "Detection and proof" and the numbered
/// sequence): the silence (one staleness floor, marked on the pruner's gossip-interval sweep);
/// half a floor after the mark, on the next tick, tickle round 1 (one direct connect, then the
/// connects through up to `VIA_PEER_TICKLE_FANOUT` peers at once, each to a killed process and so
/// spending the call's whole budget: `offline::tickle_round`); round 2 one floor after round 1's no-path
/// verdict, when its probes ended (the hold-down runs from that instant: offline.rs
/// `RoundEnd::ViaPeer::verdict_at_ms`), and its own round; TRUE OFFLINE, the provider's exit proof and the
/// submit on the next tick; then the recovery Build's run (node.delete, node.create through the
/// new node's start), one call budget.
pub fn node_recovery_due() -> std::time::Duration {
    let floor = rafka_mesh_entity::cadence::staleness_floor();
    let gossip = rafka_mesh_entity::cadence::gossip_interval();
    let call = match rafka_node_rpc::CallOptions::default().budget {
        rafka_node_rpc::Budget::Overall(d) => d,
        other => panic!("the default call budget is not one overall deadline: {other:?}"),
    };
    let round = rafka_node_admin_core::offline::tickle_round(call);
    floor + gossip + floor / 2 + gossip + round + floor + round + gossip + call
}

#[path = "../../../tools/test-support/own_process.rs"]
mod own_process;
mod build_log;
mod chaos_kit__restored_faults;
mod estate_handover;
mod estate_teardown;
mod fabric_build__accepted_topology;
mod fabric_build__await_attempt;
mod fabric_build__build_op;
mod fabric_certs__issuance;
mod fabric_soak__seeded;
mod fabric_states__rounds;
mod i143_acceptance_2775;
mod i143_acceptance_2776;
mod i143_acceptance_2777;
mod i143_acceptance_2779;
mod i143_acceptance_2780;
mod i143_acceptance_2782;
mod i143_acceptance_2783;
mod i143_acceptance_2784;
mod i143_acceptance_2786;
mod i143_acceptance_2787;
mod i143_acceptance_2803;
mod i143_acceptance_2803_detect;
mod i143_acceptance_2805;
mod i143_acceptance_2892;
mod i143_acceptance_2899;
mod i143_acceptance_2900;
mod i143_acceptance_2901;
mod i143_acceptance_2902;
mod i143_acceptance_2938;
mod i143_acceptance_2941;
mod i143_acceptance_2942;
mod i143_acceptance_rg2;
mod i143_acceptance_rg6;
mod i143_acceptance_rt6;
mod mesh_elections__cohort_election;
mod mesh_elections__fabric_primary;
mod mesh_elections__late_authority;
mod mesh_elections__role_cohort;
mod mesh_elections__sticky_seat;
mod mesh_identity__canonical_ids;
mod mesh_lifecycle__mesh_create;
mod mesh_lifecycle__mesh_leave_no_survivor;
mod mesh_lifecycle__mesh_leave_resume;
mod mesh_lifecycle__mesh_recover;
mod mesh_lifecycle__fabric_primary_handover;
mod mesh_membership__backbone;
mod mesh_membership__digest_sequence;
mod mesh_rpc__proof_store;
mod mesh_runtime__boot_trace;
mod mesh_runtime__container_kill;
mod mesh_runtime__rafka_time_discipline;
mod mesh_runtime__role_wedge;
mod mesh_runtime__successor_adoption;
mod mesh_shapes__live_resize;
mod mesh_shapes__role_build;
mod mesh_shapes__shape_reconcile;
mod node_lifecycle__admin_restart;
mod node_lifecycle__node_delete;
mod node_lifecycle__node_connections_delete;
mod node_lifecycle__node_replace;
mod node_lifecycle__node_restart;
mod node_lifecycle__node_stop;
mod node_rpc__apply_cuts;
mod node_rpc__restart_target;
mod node_rpc__role_context;
mod node_rpc__role_routing;
mod node_rpc__routing;
mod node_rpc__rpc_certainty;
mod node_rpc__scheduler_replay;
mod node_rpc__shared_dial;
mod node_rpc__status;
mod poc_chaos_kit;
mod rshape_burn_in;
