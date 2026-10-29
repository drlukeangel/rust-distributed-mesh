//! The crate's one integration-test executable: every test file in this directory or its family
//! subdirectories (`build/`, `chaos/`, `connections/`, `elections/`, `fabric/`, `gossip/`, `mesh/`, `node/`,
//! `rpc/`, `runtime/`) is a flat module here, so the crate links once. A stem runs alone as
//! `<exe> <stem>::` (with `--skip <other>::` when `<stem>` is a suffix of `<other>`).

#![allow(non_snake_case)]

/// The binary's process environment, set once, here, before `main` and before any thread exists.
#[ctor::ctor]
fn process_env() {
    for (k, v) in [("RDM_STALENESS_MS", "3000"), ("RDM_GOSSIP_INTERVAL_MS", "500"), ("RDM_BACKBONE_INTERVAL_MS", "500")] {
        if std::env::var_os(k).is_none() {
            std::env::set_var(k, v);
        }
    }
}

/// Only this story's spans reached the exporter: every one is RDM's own (`rdm.*`). A transport
/// internal (iroh, noq) or any other foreign span in a cell's trace is a defect of the capture.
pub fn assert_own_story<'a>(names: impl IntoIterator<Item = &'a str>) {
    let foreign: std::collections::BTreeSet<&str> = names.into_iter().filter(|n| !n.starts_with("rdm.")).collect();
    assert!(foreign.is_empty(), "spans outside the cell's story were exported: {foreign:?}");
}

/// How long after a member's exact runtime is killed its mesh-primary's node recovery has put a
/// new node, ready, at its path.name (node-recovery.md "Detection and proof" and the numbered
/// sequence): the silence (one staleness floor, marked on the pruner's gossip-interval sweep);
/// half a floor after the mark, on the next tick, tickle ladder 1 (one direct connect, then the
/// connects through up to `VIA_PEER_TICKLE_FANOUT` peers at once, each to a killed process and so
/// spending the call's whole budget: `offline::tickle_ladder`); TRUE OFFLINE at its fold, the provider's
/// inspection and the submit on that tick (node-recovery.md steps 2-3: nothing holds); then the recovery
/// Build's run (node.delete, node.create through the new node's start), one call budget. The bound keeps
/// one floor and one ladder beyond that sequence, the second ladder the tick ran before Q181, as slack.
pub fn node_recovery_due() -> std::time::Duration {
    let floor = rdm_core::cadence::staleness_floor();
    let gossip = rdm_core::cadence::gossip_interval();
    let call = match rdm_transport::CallOptions::default().budget {
        rdm_transport::Budget::Overall(d) => d,
        other => panic!("the default call budget is not one overall deadline: {other:?}"),
    };
    let state_key = rdm_admin::offline::tickle_ladder(call);
    floor + gossip + floor / 2 + gossip + state_key + floor + state_key + gossip + call
}

#[path = "../../../tools/test-support/own_process.rs"]
mod own_process;
mod build_log;
#[path = "chaos/restored_faults.rs"]
mod restored_faults;
mod estate_handover;
mod estate_teardown;
#[path = "build/accepted_topology.rs"]
mod accepted_topology;
#[path = "build/await_attempt.rs"]
mod await_attempt;
#[path = "build/build_op.rs"]
mod build_op;
#[path = "build/executables.rs"]
mod executables;
#[path = "build/mesh_self_heal.rs"]
mod mesh_self_heal;
#[path = "fabric/issuance.rs"]
mod issuance;
#[path = "fabric/cert_renewal.rs"]
mod cert_renewal;
#[path = "fabric/seeded.rs"]
mod seeded;
#[path = "fabric/fabric_states.rs"]
mod fabric_states;
#[path = "fabric/declared_secrets.rs"]
mod declared_secrets;
#[path = "fabric/named_listeners.rs"]
mod named_listeners;
#[path = "fabric/topology_storage_provider.rs"]
mod topology_storage_provider;
#[path = "fabric/fabric_delete.rs"]
mod fabric_delete;
#[path = "fabric/fabric_atlas.rs"]
mod fabric_atlas;
#[path = "fabric/own_delete_matrix.rs"]
mod own_delete_matrix;
#[path = "rpc/proof_holder_lifecycle.rs"]
mod proof_holder_lifecycle;
#[path = "runtime/manifest_replay.rs"]
mod manifest_replay;
#[path = "node/restart_canary_seed.rs"]
mod restart_canary_seed;
#[path = "chaos/action_generator.rs"]
mod action_generator;
#[path = "chaos/failpoint_explorer.rs"]
mod failpoint_explorer;
#[path = "chaos/wedge_detector.rs"]
mod wedge_detector;
#[path = "chaos/process_fault_backend.rs"]
mod process_fault_backend;
#[path = "chaos/container_fault_backend.rs"]
mod container_fault_backend;
#[path = "fabric/operation_ledger.rs"]
mod operation_ledger;
#[path = "fabric/driver_reconcile.rs"]
mod driver_reconcile;
#[path = "mesh/node_admin_loss_recovery.rs"]
mod node_admin_loss_recovery;
#[path = "mesh/peer_mesh_investigation.rs"]
mod peer_mesh_investigation;
#[path = "mesh/bootstrap_pending.rs"]
mod bootstrap_pending;
#[path = "elections/primary_loss_takeover.rs"]
mod primary_loss_takeover;
#[path = "runtime/supplied_clock.rs"]
mod supplied_clock;
#[path = "gossip/delta_gap_recovery.rs"]
mod delta_gap_recovery;
#[path = "gossip/channel_member.rs"]
mod channel_member;
#[path = "gossip/held_status_join.rs"]
mod held_status_join;
#[path = "gossip/partition_rejoin.rs"]
mod partition_rejoin;
#[path = "rpc/proxy_cutback.rs"]
mod proxy_cutback;
#[path = "node/restarted_member_rejoin.rs"]
mod restarted_member_rejoin;
#[path = "node/node_heard_again.rs"]
mod node_heard_again;
#[path = "node/node_recovery_backoff.rs"]
mod node_recovery_backoff;
#[path = "node/silent_node_view.rs"]
mod silent_node_view;
#[cfg(feature = "file-backend")]
#[path = "node/offline_tick_store_reads.rs"]
mod offline_tick_store_reads;
#[cfg(feature = "file-backend")]
#[path = "node/executor_pass_store_reads.rs"]
mod executor_pass_store_reads;
#[path = "build/node_create_after_delete.rs"]
mod node_create_after_delete;
#[path = "mesh/stopped_last_admin.rs"]
mod stopped_last_admin;
#[path = "gossip/peer_mesh_topology.rs"]
mod peer_mesh_topology;
#[path = "build/joiner_build_facts.rs"]
mod joiner_build_facts;
#[path = "rpc/topology_get.rs"]
mod topology_get;
#[path = "elections/node_type_election.rs"]
mod node_type_election;
#[path = "elections/fabric_primary.rs"]
mod fabric_primary;
#[path = "elections/fabric_takeover.rs"]
mod fabric_takeover;
#[path = "elections/fabric_primary_loss.rs"]
mod fabric_primary_loss;
#[path = "elections/late_lower_node_id_admin.rs"]
mod late_lower_node_id_admin;
#[path = "elections/role_election.rs"]
mod role_election;
#[path = "elections/node_primary_update.rs"]
mod node_primary_update;
#[path = "mesh/canonical_ids.rs"]
mod canonical_ids;
#[path = "mesh/mesh_create.rs"]
mod mesh_create;
#[path = "mesh/mesh_delete_no_survivor.rs"]
mod mesh_delete_no_survivor;
#[path = "mesh/mesh_delete_resume.rs"]
mod mesh_delete_resume;
#[path = "mesh/admins_lost_mid_build.rs"]
mod admins_lost_mid_build;
#[path = "mesh/fabric_primary_handover.rs"]
mod fabric_primary_handover;
#[path = "gossip/backbone.rs"]
mod backbone;
#[path = "gossip/digest_sequence.rs"]
mod digest_sequence;
#[path = "rpc/rpc_proof_store.rs"]
mod rpc_proof_store;
#[path = "runtime/boot_trace.rs"]
mod boot_trace;
#[path = "runtime/container_kill.rs"]
mod container_kill;
#[path = "runtime/rafka_time_discipline.rs"]
mod rafka_time_discipline;
#[path = "runtime/role_wedge.rs"]
mod role_wedge;
#[path = "runtime/provider_state.rs"]
mod provider_state;
#[path = "runtime/successor_adoption.rs"]
mod successor_adoption;
#[path = "build/live_resize.rs"]
mod live_resize;
#[path = "build/role_build.rs"]
mod role_build;
#[path = "build/reconcile_variants.rs"]
mod reconcile_variants;
#[path = "build/shape_reconcile.rs"]
mod shape_reconcile;
#[path = "node/admin_restart.rs"]
mod admin_restart;
#[path = "node/admin_reads_from_atlas.rs"]
mod admin_reads_from_atlas;
#[cfg(feature = "file-backend")]
#[path = "node/mesh_primary_store_reads.rs"]
mod mesh_primary_store_reads;
#[path = "node/mesh_primary_reads.rs"]
mod mesh_primary_reads;
#[path = "node/non_primary_reads.rs"]
mod non_primary_reads;
#[path = "node/node_delete.rs"]
mod node_delete;
#[path = "node/node_connections_delete.rs"]
mod node_connections_delete;
#[path = "connections/acceptance.rs"]
mod connections_acceptance;
#[path = "connections/restart.rs"]
mod connections_restart;
#[path = "node/node_replace.rs"]
mod node_replace;
#[path = "node/node_restart.rs"]
mod node_restart;
#[path = "node/node_row_status.rs"]
mod node_row_status;
#[path = "node/node_stop.rs"]
mod node_stop;
#[path = "rpc/apply_cuts.rs"]
mod apply_cuts;
#[path = "rpc/restart_target.rs"]
mod restart_target;
#[path = "rpc/role_context.rs"]
mod role_context;
#[path = "rpc/role_routing.rs"]
mod role_routing;
#[path = "rpc/routing.rs"]
mod routing;
#[path = "rpc/rpc_certainty.rs"]
mod rpc_certainty;
#[path = "rpc/scheduler_replay.rs"]
mod scheduler_replay;
#[path = "rpc/shared_dial.rs"]
mod shared_dial;
#[path = "rpc/status.rs"]
mod status;
mod poc_chaos_kit;
mod rshape_burn_in;

// ── Testkit functional cells ──
mod common;
mod container_pipeline;
mod pre_node_ready;
mod pre_node_ready_admin;
mod proof_store;
mod join_pre_node_ready;
mod member_cert;
mod member_cert_admin;
mod app_protocol_estate;
mod pipeline_rerun;
mod probe_endpoint_close;
mod probe_entry_admin;
mod probe_view_entry;
mod probe_root_span;
mod process_pipeline;
mod process_pipeline_failure;
mod rafka_time_handle;
mod node_own_config;
mod node_row_read;
mod ready_prerequisites;
mod recover_pipeline;
mod node_delete_admission;
mod node_delete_pipeline;
mod status_probe_apply;
mod stop_start;
mod rejoin_commander;
mod gossip_shutdown_once;
mod mesh_delete_member;
mod rig_teardown;
mod apply_node_name;
mod rejoin_name;
mod command_natural_key;
mod transport_diag;
mod connections_rename;
mod rename_precondition;
mod connections_delete;
mod connections_all;
mod no_caller_check;
mod node_reads;
mod node_start_sequence;
mod admin_top_up;
mod admin_start_declares;
mod stopped_birth_signal;
mod stop_during_start_read;
mod stop_reconciling;
mod admin_boot_name;
mod process_env;
mod role_process;
#[path = "gossip/receive_rules.rs"]
mod receive_rules;
#[path = "gossip/operation_facts.rs"]
mod operation_facts;

#[path = "gossip/lifecycle_facts.rs"]
mod lifecycle_facts;

#[path = "gossip/concern_reserved.rs"]
mod concern_reserved;

#[path = "gossip/digest_replacement.rs"]
mod digest_replacement;

#[path = "gossip/publication_receive.rs"]
mod publication_receive;

#[path = "gossip/reserved_receive.rs"]
mod reserved_receive;

#[path = "gossip/recovery_receive.rs"]
mod recovery_receive;

/// The synthetic probe owns this birth; it is never inferred from a path or endpoint.
fn gossip_author(fabric: &rdm_core::FabricId) -> rdm_transport::envelope::Birth {
    static BIRTH: std::sync::OnceLock<(rdm_core::NodeId, rdm_core::IncarnationId)> = std::sync::OnceLock::new();
    let (node, incarnation) = BIRTH.get_or_init(|| (rdm_core::NodeId::mint(), rdm_core::IncarnationId::mint()));
    rdm_transport::envelope::Birth { fabric: fabric.clone(), node: node.clone(), incarnation: incarnation.clone() }
}

fn declare_gossip_probe(topology_view: &rdm_transport::topology::TopologyView) {
    let birth = gossip_author(topology_view.fabric_id());
    topology_view.set_author(&rdm_core::PublisherId { node: "mesh1.rpc.9".into(), node_id: birth.node, incarnation: birth.incarnation });
}

#[path = "gossip/envelope_receive.rs"]
mod envelope_receive;
#[path = "gossip/trace_context.rs"]
mod trace_context;

/// Encoding arguments for a synthetic sender: its supplied owned birth and declared fixture revision.
async fn forward_gossip_probe(topology_view: &rdm_transport::topology::TopologyView, frame: &rdm_transport::topology::Frame) -> anyhow::Result<()> {
    if frame.context().is_some() { return topology_view.forward(frame).await; }
    let author = match frame.data() {
        rdm_transport::topology::Frame::Digest { digest } => rdm_transport::envelope::Birth { fabric: digest.fabric_id.clone(), node: digest.node.node_id.clone(), incarnation: digest.node.incarnation.clone() },
        _ => topology_view.author_birth().expect("the synthetic publisher supplied its owned birth"),
    };
    topology_view.forward(&frame.clone().authored(&author, 1, 1)).await
}

mod app_channels;
mod load;
