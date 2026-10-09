//! The crate's one integration-test executable: every file in this directory is a module
//! here, so the crate links once. A stem runs alone as `<exe> <stem>::`.

#![allow(non_snake_case)]

#[path = "../../../tools/test-support/own_process.rs"]
mod own_process;
mod estate_teardown;
mod fabric_build__accepted_topology;
mod fabric_build__await_attempt;
mod fabric_soak__seeded;
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
mod mesh_lifecycle__mesh_recover;
mod mesh_lifecycle__mesh_replace;
mod mesh_membership__backbone;
mod mesh_rpc__proof_store;
mod mesh_runtime__boot_trace;
mod mesh_runtime__container_kill;
mod mesh_runtime__role_wedge;
mod mesh_runtime__successor_adoption;
mod mesh_shapes__live_resize;
mod mesh_shapes__role_build;
mod mesh_shapes__shape_reconcile;
mod node_lifecycle__node_delete;
mod node_lifecycle__node_replace;
mod node_lifecycle__node_restart;
mod node_rpc__restart_fence;
mod node_rpc__role_context;
mod node_rpc__role_routing;
mod node_rpc__routing;
mod node_rpc__rpc_certainty;
mod node_rpc__status;
mod poc_chaos_kit;
mod rshape_burn_in;
