//! Connections, end to end on the process provider: every acceptance test of
//! `docs/architecture/connections.md` §13, every invariant of §15 and every numbered step and
//! refusal of `lifecycles/node-connections-admit.md` and `lifecycles/node-connections-delete.md`,
//! mapped to the cell that proves it. A row's "any layer" column names the cells that prove it
//! below the process provider (unit, in-process transport, in-process node); its "process" column
//! names the process-provider cell (an estate of real node processes, `MESH_SPAWN_TYPE=process`).
//! A row whose process column is "none" is proved in this section, by the cell named in the last
//! column, or carries the reason it is not writable here. The cell that kills and recovers the source
//! (§13 B and C, Q227) is in `connections/restart.rs`, the release-only stem `connections_restart`;
//! the rest are this stem's.
//!
//! Crate keys: `t:` rdm-transport/tests, `n:` rdm-node/tests, `a:` rdm-admin/tests, `c:` rdm-core/tests,
//! `p:` rdm-test/tests (a path under it). `p:rpc/proxy_cutback.rs` runs in the fast gate as
//! the job `i143-2938-process` (tools/mesh-audit/i143-acceptance-jobs.json, started by
//! scripts/rdm-gate-at.sh; its stem is in `SCENARIO_SKIP`, so the sharded main job does not run it twice).
//!
//! The record of every row, with each cell's line, stem and tier, is `docs/plans/connections-coverage.md`.
//!
//! # connections.md §13, required acceptance tests
//!
//! | test | any layer | process | this section |
//! |---|---|---|---|
//! | A one discovered proxy serves every consumer | `n:connection_reconnect::one_discovered_proxy_serves_every_consumer_of_the_same_destination`; `t:connections::a_held_proxy_is_taken_without_a_direct_dial`; `t:connections_integration::an_active_proxy_is_reused_without_a_fresh_direct_dial_ladder` | none | `one_discovered_proxy_carries_every_later_open_with_no_direct_dial` |
//! | B a restarted source holds no Proxy (Q227) | `n:connection_reconnect::a_restarted_source_holds_no_proxy_and_proves_its_carrier_again`; `a:connections_writer::a_restart_holds_none_of_the_previous_births_rows_and_the_first_resolution_dials` | none | `connections/restart.rs::a_recovered_source_holds_no_connection_and_reads_none_back` |
//! | C a restart starts reconnect backoff fresh (Q227) | `n:connection_reconnect::a_restarted_source_holds_no_reconnect_obligation_and_reads_no_failed_row_back`; `c:reconnect::the_backoff_is_computed_from_the_latest_failed_row_held_not_reset`; `a:connections_writer::a_restarted_birth_starts_its_own_series_naming_its_own_birth` | none | `connections/restart.rs::a_recovered_source_holds_no_connection_and_reads_none_back` (release) |
//! | D index stays bounded | `n:connection_reconnect::the_connections_index_stays_bounded_while_the_raw_log_keeps_history`; `a:connections_writer::many_failures_grow_the_raw_log_and_keep_the_index_at_one_direct_per_pair`; `a:connections_reconnect::source_reconnect_failures_grow_log_keep_two_current_members` | none | `many_failed_attempts_grow_the_raw_log_and_hold_one_direct_and_one_proxy` |
//! | E Failed/Dead stay with their source | `c:connections::a_disconnected_or_failed_direct_never_stays_resident_and_an_older_connect_never_revives_it`; `c:connections::another_nodes_proxy_is_never_held` | none | `a_peer_holds_none_of_a_sources_failed_history_or_its_proxy` |
//! | F reconnect success returns to Direct in order | `a:connections_writer::a_direct_connected_beside_a_proxy_disconnects_it_durably_and_the_route_cuts_back`; `t:connections::a_direct_connected_disconnects_the_proxy`; `t:connections_integration::a_direct_connected_owes_the_proxy_disconnected_write_and_new_calls_cut_back_only_after_it_is_durable`; `c:reconnect::cutback_to_direct_happens_only_after_the_proxy_disconnected_write_is_applied_and_a_failed_write_is_owed_again` | `p:rpc/proxy_cutback::source_accept_wakes_its_own_dial_whose_direct_disconnects_proxy_before_cutback` (its dial is woken by an accept, not by the series' due time) | `a_restored_direct_disconnects_the_proxy_before_the_next_open_goes_direct` |
//! | G an inbound accept is a recovery trigger | `a:connections_writer::an_accept_from_the_proxy_destination_writes_nothing_and_wakes_the_source_dial_whose_connected_disconnects_the_proxy`; `a:connections_writer::repeated_accepts_coalesce_into_one_attempt_that_waits_for_the_series_backoff`; `t:connection_observer::an_accepted_connection_is_handed_to_the_accept_trigger_once` | `p:rpc/proxy_cutback::source_accept_wakes_its_own_dial_whose_direct_disconnects_proxy_before_cutback` | - |
//! | H Proxy retirement write failure is safe and retried | `a:connections_writer::a_refused_disconnected_write_keeps_the_proxy_effective_until_it_lands`; `n:connection_writes::a_refused_connection_write_is_retried_with_backoff_until_it_lands` | `p:rpc/proxy_cutback::source_accept_wakes_its_own_dial_whose_direct_disconnects_proxy_before_cutback` | - |
//! | I destination/carrier incarnation | `c:connections::a_failed_direct_naming_a_superseded_destination_stands_no_series_and_the_call_dials` (added with its fix); `c:connections::a_stale_destination_incarnation_is_superseded`; `c:connections::a_stale_carrier_incarnation_is_superseded`; `c:connections::a_direct_fact_naming_a_superseded_destination_is_not_current`; `t:connections_integration::a_fact_naming_a_superseded_birth_still_dials_the_current_one`; `t:connections::an_open_against_another_incarnation_is_wrong_incarnation_and_never_served`; `a:connections_writer::a_failed_dial_to_the_destinations_next_birth_starts_a_new_series_naming_that_birth` | none | `a_proxy_naming_a_replaced_destination_birth_is_disconnected_by_name_and_never_carries`; `a_proxy_naming_a_replaced_carrier_birth_is_disconnected_by_name_and_never_carries` |
//! | J carrier edge loss invalidates Proxy | `t:forward::a_carrier_whose_own_edge_to_the_target_is_not_active_refuses_carrier_edge_lost`; `t:forward::a_carried_call_answering_carrier_edge_lost_hands_the_proxy_back_to_be_disconnected`; `a:connections_writer::a_carrier_names_its_own_edge_only_when_its_latest_direct_is_not_connected` | none | `a_carrier_that_lost_its_edge_refuses_by_name_and_the_source_disconnects_the_proxy` |
//! | K sent request is never rerouted | `t:connections_integration::an_indeterminate_alone_never_invalidates_a_proxy`; `t:routing::certainty_stays_downstream_an_indeterminate_is_never_replayed_through_another_route`; `t:certainty::reply_loss_after_a_complete_send_is_indeterminate` | none | not writable here: no process door forces a post-commit answer timeout on a carried call (Decisions) |
//! | L #2640 independence | `t:routing::semantic_selection_is_not_connection_selection`; `t:routing::connection_changes_do_not_touch_the_selection_and_selection_changes_fabricate_no_connection_state` | none | n/a in RDM: no `orgs.topics`, route barrier or watermark exists here; the transport cells prove the selection is untouched |
//!
//! # connections.md §15, invariants
//!
//! | # | any layer | process | this section |
//! |---|---|---|---|
//! | 1 one durable history | `t:connections::a_direct_open_writes_connected_with_its_alpn_and_the_acceptor_sees_the_same_row_id` | `p:rpc/proxy_cutback` (its store root's log) | every cell reads the one log (`Estate::connection_history`) |
//! | 2 generic across node types | `c:connections::carriers_are_asked_from_the_topology_in_path_order_spread_by_source` | `p:rpc/role_routing::a_gateway_reaches_the_selected_broker_direct_via_a_peer_gateway_or_not_at_all` | - |
//! | 3 Direct and Proxy kinds | `c:connections::malformed_entries_are_refused_by_name` | `p:rpc/proxy_cutback` | A, F |
//! | 4 state to virtual classification | none: RDM declares no virtual classification (§3.2); its file backend is one `connections.jsonl` (§3) | none | page-vs-code difference, reported |
//! | 5 raw log and one latest per kind | as D | none | D |
//! | 6 index never one member per retry | as D | none | D |
//! | 7 a node holds its own facts only, read from no store | as E | none | E |
//! | 8 active Proxy is source-owned | `c:connections::another_nodes_proxy_is_never_held` | none | E |
//! | 9 Failed/Dead durable, read by the node investigation, held by no other node | `c:connections::a_disconnected_or_failed_direct_never_stays_resident_and_an_older_connect_never_revives_it` | none | E |
//! | 10 a valid Proxy overrides Direct until durably retired | `c:connections::the_route_prefers_a_valid_proxy_then_an_active_direct_then_none`; as F, H | `p:rpc/proxy_cutback::source_accept_wakes_its_own_dial_whose_direct_disconnects_proxy_before_cutback` | F |
//! | 11 every node repairs its own direct connections | `n:connection_reconnect::one_reconnect_attempt_at_a_time_per_node`; `a:connections_writer::a_failed_row_arms_the_due_time_reconciler_without_an_accept_reschedules_on_failure_and_coalesces_with_accepts` | none | C, F (the dial is the source process's own span) |
//! | 12 node-admin schedules no reconnect | none (an absence) | none | C, F (no node-admin process emits a recovery dial) |
//! | 13 not a platform Job | n/a: a structure, not a behaviour | - | - |
//! | 14 the latest Failed row held computes the series | `c:reconnect::the_backoff_is_computed_from_the_latest_failed_row_held_not_reset`; `c:reconnect::a_new_incident_starts_a_new_round_at_ordinal_one` | none | C |
//! | 15 a restart starts backoff fresh | as C | none | C |
//! | 16 a Direct Connected creates the retirement obligation | as F, G | `p:rpc/proxy_cutback` | F |
//! | 17 failed retirement writes are retried | as H | `p:rpc/proxy_cutback` | - |
//! | 18 incarnations make stale evidence refusable | as I | none | I |
//! | 19 a failed attempt is not node-death proof | `c:reconnect::route_resolution_never_creates_or_moves_a_reconnect` | none | A (the destination stays ready in node.get) |
//! | 20 traffic does not probe Direct while a Proxy is active | as A | none | A |
//! | 21 a sent invocation is never rerouted | as K | none | as K |
//! | 22 independent of `orgs.topics` | as L | none | as L |
//! | 23 `durable format moved` | n/a: a landing rule | - | - |
//! | 24 #2722 authorization | n/a: a permission | - | - |
//!
//! # connections.md §1-§12, the statements the tests above lean on
//!
//! | statement | any layer | process | this section |
//! |---|---|---|---|
//! | §1 every write is asynchronous (Q169) | `n:connection_writes::an_open_returns_before_its_row_lands_on_a_store_reached_over_that_connection`; `n:connection_writes::a_start_hold_and_a_settlement_never_wait_behind_a_refused_writes_backoff` | none | - |
//! | §1.1 control plane is no connection | `p:connections_all::a_standalone_all_closes_the_log_connections_and_leaves_the_node_heard_on_gossip` | none | delete rows below |
//! | §1.2 `Refused` is the acceptor's fact | `t:connections::a_refusal_is_a_refused_fact_on_the_acceptor_and_a_failed_row_on_the_dialer` | none | the verdict and span: `a_dial_presenting_no_member_cert_is_refused_no_credential_on_the_acceptor`; the row is written only for a dialer the acceptor's topology view names, as node-connections-admit.md's Row says (Q212) |
//! | §2 a row carries its ALPN | `t:connections::a_direct_open_writes_connected_with_its_alpn_and_the_acceptor_sees_the_same_row_id` | none | A |
//! | §3.1 reachability is per protocol | `t:connections::a_working_alpn_a_proves_nothing_for_alpn_b_and_a_reconnect_on_a_never_disconnects_bs_proxy`; `n:connection_reconnect::alpn_interference_restores_a_while_b_keeps_its_proxy_from_the_shared_store` | none | - |
//! | §5.1 precedence; only a Failed latest waits for its series | `c:connections::the_route_prefers_a_valid_proxy_then_an_active_direct_then_none`; `c:connections::a_dropped_direct_is_dialled_again_and_only_a_failed_one_waits_for_its_series` | none | F |
//! | §5.2 a route change affects new invocations only | `t:connections::an_open_tunnel_and_its_streams_outlive_direct_restoration_and_the_next_open_goes_direct` | none | - |
//! | §6 carrier policy and deterministic spread | `t:connections::the_carrier_choice_is_kept_until_a_named_event_and_spread_by_source`; `t:connections::a_protocol_with_proxy_false_never_consumes_a_proxy_even_when_one_is_held`; `t:connections::a_carrier_outside_the_protocols_list_is_never_picked`; `c:connections::a_valid_proxy_through_a_carrier_kind_the_protocol_forbids_is_skipped_not_disconnected` | none | - |
//! | §7 establishing a Proxy (Failed, carrier, tunnel `admitted`, Proxy Connected) | `t:connections::a_not_sent_direct_on_a_proxy_alpn_goes_once_through_the_hashed_carrier`; `t:connections::the_tunnel_handshake_carries_the_destinations_admitted_at_the_exact_birth_and_only_then_writes_proxy_connected`; `t:connections::a_tunnel_refused_wrong_incarnation_or_lapsed_writes_nothing_and_ends_by_name` | none | A |
//! | §8 validity and named invalidation | as I, J | none | I, J |
//! | §9.4 one scheduler, one pending item | `n:connection_reconnect::one_reconnect_attempt_at_a_time_per_node` | none | - |
//! | §9.5 one attempt: verify birth, verify not superseded, dial, Failed or Connected | `a:connections_writer::an_accept_from_another_birth_arms_nothing_and_an_attempt_for_a_gone_birth_is_dropped_undialled`; `a:connections_writer::a_failed_dial_after_an_accept_keeps_the_proxy_and_reschedules_from_its_own_failed_row` | none | C, F |
//! | §10 a stream Proxy outlives its retirement | `t:connections::an_open_tunnel_and_its_streams_outlive_direct_restoration_and_the_next_open_goes_direct` | none | - |
//! | §12 transition and admission spans | `a:connections_writer::every_landed_connection_transition_is_spanned_with_its_pair_kind_state_reason_carrier_and_series_position`; `a:connections_writer::a_pending_reconnect_attempt_never_dialled_ends_on_a_span_naming_its_superseded_birth_or_the_direct_connected_that_satisfied_it`; `t:connections::a_dial_parents_to_the_callers_span_and_the_stream_bytes_are_not_spanned`; `t:connections::an_inbound_connection_on_a_registered_alpn_is_admitted_on_a_valid_cert`; `t:connections::a_refused_dial_is_spanned_with_the_presented_certs_expiry_and_the_clock_it_was_judged_on` | none | every cell asserts its spans by name |
//! | §12 `effective_route_cold_discovery_ms` | none | none | A (the cold tunnel names it; the tunnel over the held Proxy does not) |
//!
//! # lifecycles/node-connections-admit.md, fabric-certs.md "On a connection"
//!
//! | step / refusal | any layer | process | this section |
//! |---|---|---|---|
//! | registered ALPNs only; never Node RPC or gossip | `t:connections::an_inbound_connection_on_a_registered_alpn_passes_admit_before_accept`; `t:connections::a_node_rpc_and_a_gossip_connection_carry_no_admission_span` | `p:app_protocol_estate::the_day0_admin_and_an_rpc_node_dial_each_other_on_an_app_protocol_in_a_one_mesh_fabric` | - |
//! | a registration naming RDM's own ALPNs refused at `bind` | `t:connections::registering_the_step_for_node_rpc_or_gossip_is_refused_at_bind`; `a:admin_app_alpns::binding_rdm_own_alpn_through_wiring_is_refused` | none (a start refusal: the in-process admin is its process) | - |
//! | step 1 QUIC on a registered ALPN, key possession | iroh's (n/a) | - | - |
//! | step 2 the dialer presents its most recently installed cert | `p:fabric/cert_renewal::a_member_is_renewed_at_the_policys_point_and_dials_past_its_first_expiry_admitted` | none | - |
//! | step 3 read within one floor | `t:connections::a_dial_that_presents_nothing_within_the_floor_is_no_credential` | none | - |
//! | step 4 `admitted`, served; `Connected` fact | `t:connections::an_inbound_connection_on_a_registered_alpn_is_admitted_on_a_valid_cert`; `a:app_protocol_dials::the_day0_root_and_an_ordinary_node_it_launched_dial_each_other_on_an_app_protocol` | `p:app_protocol_estate` (both cells) | - |
//! | step 4 `refused { reason }`, `Refused` fact, closed | `t:connections::a_refusal_is_a_refused_fact_on_the_acceptor_and_a_failed_row_on_the_dialer` | none | `a_dial_presenting_no_member_cert_is_refused_no_credential_on_the_acceptor` (verdict and span; the row as §1.2) |
//! | step 5 the dialer reads the verdict; its reconnect rule follows | `n:connection_reconnect::a_refused_dial_starts_a_series_a_refused_attempt_continues_and_an_admitted_one_ends` | none | - |
//! | `no-credential` | `t:connections::a_dial_that_presents_nothing_within_the_floor_is_no_credential`; `a:app_protocol_dials::an_admins_app_protocol_refuses_a_dialer_holding_no_cert_or_an_expired_one_by_name` | none | `a_dial_presenting_no_member_cert_is_refused_no_credential_on_the_acceptor` |
//! | `ca-bad-signature` | `t:connections::an_inbound_connection_is_refused_by_name_on_ca_bad_signature` | none | `a_dial_presenting_another_fabrics_cert_is_refused_ca_bad_signature_on_the_acceptor` |
//! | `ca-expired` | `t:connections::an_inbound_connection_is_refused_by_name_on_ca_expired` | none | - (one process refusal of each class proves the acceptor's path; the chain checks are the transport cells') |
//! | `issuer-bad-signature` | `t:connections::an_inbound_connection_is_refused_by_name_on_issuer_bad_signature` | none | - (as `ca-expired`) |
//! | `issuer-expired` | `t:connections::an_inbound_connection_is_refused_by_name_on_issuer_expired` | none | - (as `ca-expired`) |
//! | `issuer-mesh-mismatch` | `t:connections::an_inbound_connection_is_refused_by_name_on_issuer_mesh_mismatch` | none | - (as `ca-expired`) |
//! | `bad-signature` | `t:connections::an_inbound_connection_is_refused_by_name_on_bad_signature` | none | - (as `ca-expired`) |
//! | `expired` | `t:connections::an_inbound_connection_is_refused_by_name_on_expired`; `a:app_protocol_dials::an_admins_app_protocol_refuses_a_dialer_holding_no_cert_or_an_expired_one_by_name` | none | - (as `ca-expired`) |
//! | Never: nothing inferred about the dialer | none | none | the refusal cells: the refused dialer's path.name stays ready |
//! | `NoCerts` admits by name | `t:connections::a_no_certs_acceptor_admits_by_name`; `p:member_cert::a_mesh_primary_configured_with_no_certs_admits_the_birth_with_an_empty_cert_and_says_so` | none | - |
//! | a read the acceptor cannot complete writes nothing | `t:connections::a_dial_that_presents_nothing_within_the_floor_is_no_credential` (its reset arm) | none | - |
//! | spans `via-admitted`, `via-refused`, `via-presented` | `t:connections::an_inbound_connection_on_a_registered_alpn_is_admitted_on_a_valid_cert`; `t:connections::a_refused_dial_is_spanned_with_the_presented_certs_expiry_and_the_clock_it_was_judged_on` | none | the refusal cells |
//!
//! # lifecycles/node-connections-delete.md
//!
//! | step / refusal | any layer | process | this section |
//! |---|---|---|---|
//! | step 0 a primary target runs node.primary.update first (`All`) | inside a replace: `p:elections/node_primary_update::a_planned_hand_over_makes_the_same_mesh_successor_both_primaries_on_its_direct_answer`; inside a stop: `p:node/node_stop::a_stop_of_a_mesh_primary_hands_over_by_node_primary_update_before_it_stops` | none for a standalone `node.connection.delete` | `a_mesh_primarys_connections_delete_hands_its_primary_over_before_its_all` |
//! | step 1 the exact birth, the scope | `p:node/node_connections_delete::a_standalone_connections_delete_completes_on_connections_deleted_and_leaves_the_process_running` | as any layer | - |
//! | step 2 `Connections([…])`: exactly those closed, unknown and already-closed named | `p:connections_delete::a_connections_delete_naming_exact_ids_cuts_only_those_and_names_every_id` | `p:node/node_connections_delete::a_mesh_primary_named_in_its_own_connections_delete_runs_its_own_command_path_and_records_from_its_own_frames` (an id never opened) | `a_connections_delete_by_id_closes_that_connection_and_names_the_rest` |
//! | step 2 an empty list refused by name | `p:connections_delete::a_connections_delete_naming_exact_ids_cuts_only_those_and_names_every_id`; `rdm-core status.rs` unit | none | `a_connections_delete_by_id_closes_that_connection_and_names_the_rest` |
//! | step 3.1 block, report `ConnectionsDeleting`, then the record | `p:command_natural_key::the_node_answers_a_connections_delete_on_its_command_stream_in_order_for_both_scopes` | `p:node/node_connections_delete::a_connections_delete_is_admitted_by_the_node_on_its_command_stream_and_nothing_is_recorded_before_the_send` | - |
//! | steps 3.2-3.3 admitted work finishes | `p:connections_all::all_closes_the_connections_opened_while_it_waited_for_admitted_work` | none | - |
//! | step 3.4 every open Direct or Proxy closed | `p:connections_all::a_standalone_all_closes_the_log_connections_and_leaves_the_node_heard_on_gossip`; `p:connections_delete::an_all_whose_drops_cannot_land_answers_at_once_and_closes_every_live_connection` | none | `an_all_closes_every_data_plane_connection_and_the_node_refuses_new_work_by_name` |
//! | step 3.5 drops under the current name | `p:connections_rename::a_renamed_nodes_next_connections_entries_name_the_name_it_holds_at_both_ends` | none | `an_all_closes_every_data_plane_connection_and_the_node_refuses_new_work_by_name` |
//! | step 3.6 `Established` and the closed ids | `p:command_natural_key::the_node_answers_a_connections_delete_on_its_command_stream_in_order_for_both_scopes` | `p:node/node_connections_delete::a_connections_delete_is_admitted_by_the_node_on_its_command_stream_and_nothing_is_recorded_before_the_send` | - |
//! | step 4 connections-deleted set from `Established` | `a:status_rpc::a_deadline_all_leaves_the_row_connections_deleting_and_a_new_attempt_ending_established_sets_connections_deleted` (and step 5's row) | `p:node/node_row_status::a_node_row_carries_its_status_through_ready_stop_and_start` | - |
//! | step 5 `Deadline` at `RDM_CONNECTIONS_DELETE_DEADLINE_MS` | `p:command_natural_key::an_all_with_work_that_never_finishes_ends_at_its_bound_with_a_deadline_receipt` | none | - |
//! | step 6 nothing inferred: no stop, exit or node.deleted | `p:node/node_connections_delete::a_standalone_connections_delete_completes_on_connections_deleted_and_leaves_the_process_running` | as any layer | - |
//! | connections-deleting refuses all but 0x1B, 0x21, 0x22 | `a:status_rpc::connections_deleting_mesh_primary_serves_status_checks_exact_birth`; `a:admin_serves_forward::a_connections_deleting_admin_refuses_forward_by_name`; `t:forward::a_nested_forward_and_a_carrier_deleting_connections_are_still_refused_by_name` | none | `an_all_closes_every_data_plane_connection_and_the_node_refuses_new_work_by_name` |
//! | the process runs; gossip still heard | `p:connections_all::a_standalone_all_closes_the_log_connections_and_leaves_the_node_heard_on_gossip` | `p:node/node_connections_delete::a_standalone_connections_delete_completes_on_connections_deleted_and_leaves_the_process_running` | - |

use rdm_test::estate::{named, wait_for, Estate, Owner};
use serde_json::Value;
use std::time::Duration;

const SOURCE: &str = "mesh1.rpc.1";
const CARRIER: &str = "mesh1.rpc.2";
const DEST: &str = "mesh1.rpc.3";

fn owner(test: &str) -> Owner {
    Owner { product: "mesh".into(), feature: "connections".into(), subfeature: "connections-e2e".into(), rung: "MN".into(), provider: "process".into(), test: test.into() }
}

fn s(v: &Value) -> String {
    match v {
        Value::String(x) => x.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn start(sp: &Value) -> u64 {
    sp["start_unix_nano"].as_u64().unwrap_or(0)
}

/// One mesh, `{mesh1: 2 node-admins, 3 rpc nodes}`, every rpc node registering the testkit's app
/// protocol proxied through an rpc node carrier (`node::APP_PROTOCOL_PROXY_KNOB`), its
/// streams echoed; the source holds the carrier's and the destination's births.
async fn born(cell: &str) -> Estate {
    let estate = Estate::bootstrap(owner(cell), "fabric1", "mesh1").await;
    let root = estate.bootstrap_data_dir("mesh1").parent().expect("the estate root holds the data dirs").to_path_buf();
    std::fs::write(root.join(rdm_test::node::APP_PROTOCOL_PROXY_KNOB), b"").unwrap();
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 3)]).await;
    estate.settled_views(&[("mesh1", 2, 3)]).await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    for (witness, peer) in [(SOURCE, CARRIER), (SOURCE, DEST), (CARRIER, DEST), (DEST, SOURCE)] {
        estate.witness_holds_birth(witness, peer).await;
    }
    estate
}

fn stream(estate: &Estate, from: &str, to: &str, payload: &str) -> Value {
    estate.probe(&["stream", "--target", &format!("path:{from}"), "--destination", &format!("path:{to}"), "--value", payload, "--key", "x"])
}

fn block_direct(estate: &Estate, from: &str, to: &str, blocked: bool) {
    let (t, d) = (format!("path:{from}"), format!("path:{to}"));
    let mut args = vec!["block-direct", "--target", t.as_str(), "--destination", d.as_str(), "--key", "x"];
    if !blocked {
        args.push("--release");
    }
    let out = estate.probe(&args);
    assert_eq!(out["direct_blocked"], blocked, "{from}'s Direct dials toward {to} are set: {out}");
}

fn snapshot(estate: &Estate, node: &str) -> Value {
    let out = estate.probe(&["snapshot", "--target", &format!("path:{node}"), "--key", "x"]);
    assert_eq!(out["outcome"], "Reply", "{node} answers the snapshot door: {out}");
    out
}

/// The rows of `node`'s raw log toward `to` of `kind` (`Direct` or `Proxy`).
fn rows<'a>(history: &'a [Value], to: &str, kind: &str) -> Vec<&'a Value> {
    history.iter().filter(|r| r["destination"]["name"] == to && r["kind"] == kind).collect()
}

/// `node.connection.delete { Connections(ids) }` of `node`'s exact birth through its mesh-primary.
async fn connections_delete_ids(estate: &Estate, node: &str, ids: &[String]) -> rdm_core::status::StatusReply {
    let mesh = estate.nodes_obj().await.get(&rdm_client::Filter::new().eq("mesh", ["mesh1"])).await.expect("node.get [mesh]");
    let primary = mesh.iter().find(|n| n.is_primary).cloned().expect("node.get names mesh1's primary");
    let target = mesh.iter().find(|n| n.name.to_string() == node).cloned().unwrap_or_else(|| panic!("no {node} in mesh1"));
    let birth = rdm_client::ExactBirth { target: rdm_transport::NodeTarget::ExactNode(target.node_id.clone()), node_id: target.node_id.clone(), incarnation: target.incarnation_id.clone().expect("a live birth") };
    let ids = ids.iter().map(|i| rdm_core::ConnectionId::parse(i).unwrap_or_else(|e| panic!("{i} is not a connection id: {e:?}"))).collect();
    estate.nodes_obj().await.connections_delete(&birth, &primary.node_id, &rdm_transport::RouteChoice::Direct, &rdm_core::status::ConnectionScope::Connections(ids)).await.expect("node.connection.delete answered")
}

/// The carrier holds its own Direct edge to the destination on the app protocol: a stream it opened,
/// its `Direct Connected` landed on the store root, where a source's carrier discovery reads it.
async fn carrier_edge(estate: &Estate) {
    let edge = stream(estate, CARRIER, DEST, "carrier-edge");
    assert_eq!((edge["streamed"].as_bool(), edge["kind"].as_str()), (Some(true), Some("direct")), "the carrier reaches the destination directly: {edge}");
    wait_for("the carrier's Direct Connected toward the destination landed", Duration::from_secs(15), || async {
        rows(&estate.connection_history(CARRIER), DEST, "Direct").last().is_some_and(|r| r["state"] == "Connected").then_some(())
    })
    .await;
}

/// CONTRACT (connections.md §13 A: "First operation from gw1 proves and writes `Proxy Connected via
/// gw2`. A second operation, including one for a different topic that selects the same brk2, must use
/// the Proxy immediately. Forbidden: another full direct dial ladder before proxying"; §7 steps 1-6;
/// §12: "Cold discovery continues to expose `effective_route_cold_discovery_ms`"): the source's Direct
/// dials toward the destination are blocked (`NotSent`, as an unreachable dial). Its first open on the
/// app protocol writes one `Direct Failed` (round 1, ordinal 1), takes the carrier whose Direct edge to
/// the destination is active, and opens the tunnel; the destination's own `admitted` comes back and the
/// source writes `Proxy Connected` naming the carrier and both births. A second open, another
/// consumer's payload, goes through the held Proxy at once: its trace holds no Direct dial. The first
/// tunnel, the cold discovery, names how long it took (`effective_route_cold_discovery_ms`); the
/// second, over the held Proxy, discovered nothing. The destination stays ready: a failed
/// direct attempt is source-local evidence (§15.19). Must NOT happen: a second Direct Failed from an
/// open, a second Proxy Connected, or a stream answered by any node but the destination.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_discovered_proxy_carries_every_later_open_with_no_direct_dial() {
    let cell = "one_discovered_proxy_carries_every_later_open_with_no_direct_dial";
    let mut estate = born(cell).await;
    let dest = estate.node_value(DEST).await;
    let carrier = estate.node_value(CARRIER).await;
    carrier_edge(&estate).await;
    block_direct(&estate, SOURCE, DEST, true);

    // 1. The first open: Direct NotSent, the carrier found, the tunnel admitted by the destination.
    let first = stream(&estate, SOURCE, DEST, "consumer-a");
    assert_eq!((first["streamed"].as_bool(), first["kind"].as_str()), (Some(true), Some(format!("proxy via {CARRIER}").as_str())), "the first open goes through the carrier: {first}");
    assert_eq!((s(&first["peer"]), s(&first["peer_incarnation"]), s(&first["answered"])), (DEST.to_string(), s(&dest["incarnation_id"]), format!("echo {DEST}: consumer-a")), "the destination's exact birth answered: {first}");
    let held = wait_for("the source holds its Proxy once its row landed", Duration::from_secs(15), || async {
        let snap = snapshot(&estate, SOURCE);
        (snap["own_active_proxies"].as_array().map(Vec::len) == Some(1)).then_some(snap)
    })
    .await;
    let proxy = &held["own_active_proxies"][0];
    assert_eq!((s(&proxy["carrier"]["name"]), s(&proxy["carrier"]["incarnation"]), s(&proxy["destination"]["incarnation"])), (CARRIER.to_string(), s(&carrier["incarnation_id"]), s(&dest["incarnation_id"])), "the Proxy names the carrier and both births: {proxy}");

    // 2. Another consumer's open: through the held Proxy at once.
    let second = stream(&estate, SOURCE, DEST, "consumer-b");
    assert_eq!((second["streamed"].as_bool(), second["kind"].as_str(), s(&second["answered"])), (Some(true), Some(format!("proxy via {CARRIER}").as_str()), format!("echo {DEST}: consumer-b")), "{second}");

    // 3. The log: one Failed from the open, one Proxy Connected.
    let history = estate.connection_history(SOURCE);
    let proxies = rows(&history, DEST, "Proxy");
    assert_eq!(proxies.len(), 1, "one Proxy Connected: {proxies:?}");
    assert_eq!((s(&proxies[0]["state"]), s(&proxies[0]["carrier"]["name"])), ("Connected".to_string(), CARRIER.to_string()), "{proxies:?}");
    let failed = rows(&history, DEST, "Direct");
    assert!(failed.first().is_some_and(|r| r["state"] == "Failed" && r["recovery"]["attempt_ordinal"] == 1), "the series starts at ordinal 1: {failed:?}");
    assert!(failed.iter().all(|r| r["state"] == "Failed"), "nothing but Failed toward the destination while blocked: {failed:?}");
    let still = estate.node_value(DEST).await;
    assert_eq!(s(&still["status"]), "ready", "a failed direct attempt marks nothing on the destination: {still}");
    estate.stop().await;

    // 4. Spans: the two tunnels, the first a cold discovery that names its cost; no Direct dial in
    // the second open's trace.
    let spans = estate.spans();
    // The source's own tunnel spans: the carrier's tunnel span names the same source, and records no kind.
    let tunnels: Vec<&Value> = named(&spans, "rdm.node.connections.create.via-tunnel").into_iter().filter(|sp| sp["attributes"]["source"] == SOURCE && sp["attributes"]["destination"] == DEST && sp["attributes"]["kind"] == "proxy").collect();
    assert_eq!(tunnels.len(), 2, "two tunnels from the source: {tunnels:#?}");
    let (cold, held_open) = (tunnels[0], tunnels[1]);
    // Each tunnel's carrier span is its child (telemetry.md §3), exported though both tunnels are
    // still open when the estate stops: it ends once its tunnel opens, never at the tunnel's end.
    for tunnel in [cold, held_open] {
        let carried: Vec<&Value> = named(&spans, "rdm.node.connections.create.via-tunnel").into_iter().filter(|sp| sp["parent_span_id"] == tunnel["span_id"]).collect();
        assert_eq!(carried.len(), 1, "the carrier's tunnel span is the source tunnel's child: {tunnel} {carried:#?}");
        assert_eq!((s(&carried[0]["attributes"]["carrier"]), s(&carried[0]["attributes"]["entry"]), s(&carried[0]["attributes"]["outcome"])), (CARRIER.to_string(), s(&tunnel["attributes"]["entry"]), "opened".to_string()), "{}", carried[0]);
    }
    assert_eq!((s(&cold["attributes"]["carrier"]), s(&cold["attributes"]["kind"]), s(&cold["attributes"]["state"])), (CARRIER.to_string(), "proxy".to_string(), "connected".to_string()), "{cold}");
    let cost = cold["attributes"]["effective_route_cold_discovery_ms"].as_u64().or_else(|| s(&cold["attributes"]["effective_route_cold_discovery_ms"]).parse().ok());
    assert!(cost.is_some(), "the cold discovery names its cost (connections.md §12): {cold}");
    assert!(held_open["attributes"].get("effective_route_cold_discovery_ms").is_none_or(Value::is_null), "an open over the held Proxy discovered nothing: {held_open}");
    let direct_in = |trace: &Value| -> Vec<&Value> {
        named(&spans, "rdm.node_admin.connection.update.via-observed").into_iter().filter(|sp| sp["trace_id"] == *trace && sp["attributes"]["kind"] == "direct" && sp["attributes"]["source"] == SOURCE).collect()
    };
    let first_failed = direct_in(&cold["trace_id"]);
    assert_eq!(first_failed.len(), 1, "the first open wrote its one Direct Failed: {first_failed:#?}");
    assert_eq!(s(&first_failed[0]["attributes"]["state"]), "failed", "{:?}", first_failed[0]);
    assert!(direct_in(&held_open["trace_id"]).is_empty(), "the second open dialled nothing direct: {:#?}", direct_in(&held_open["trace_id"]));
    estate.record_trace_url(&s(&cold["trace_id"]));
}

/// CONTRACT (connections.md §13 E: "Generate repeated reconnect failures on gw1. Assert no peer node
/// holds any of gw1's facts: a node holds its own facts only"; §4.1, §4.2; §15.7-9; Q227): the source
/// writes a series of Direct Failed facts toward the destination (six on the control ALPN through the
/// writer's own observer path, one on the app protocol from a blocked open), all durable in the one
/// log. A peer that then opens toward the source through a carrier takes the carrier from the
/// topology it holds and reads no fact from the store: everything it holds is its own, none of the
/// source's failed history, none of the carrier's edges and none of the source's Proxy. Must NOT
/// happen: a held entry whose source is another node, or another node's Proxy held.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_peer_holds_none_of_a_sources_failed_history_or_its_proxy() {
    let cell = "a_peer_holds_none_of_a_sources_failed_history_or_its_proxy";
    let mut estate = born(cell).await;
    carrier_edge(&estate).await;
    // The carrier's edge to the source too: the peer's discovery below goes through it.
    let back = stream(&estate, CARRIER, SOURCE, "carrier-edge-to-source");
    assert_eq!((back["streamed"].as_bool(), back["kind"].as_str()), (Some(true), Some("direct")), "{back}");
    wait_for("the carrier's Direct Connected toward the source landed", Duration::from_secs(15), || async {
        rows(&estate.connection_history(CARRIER), SOURCE, "Direct").last().is_some_and(|r| r["state"] == "Connected").then_some(())
    })
    .await;

    // 1. The source's failures: six on the control ALPN, then a blocked open on the app protocol.
    let seeded = estate.probe(&["record-proxy", "--target", &format!("path:{SOURCE}"), "--destination", &format!("path:{DEST}"), "--carrier", &format!("path:{CARRIER}"), "--failed-attempts", "6", "--key", "x"]);
    assert_eq!(seeded["proxy_recorded"], true, "{seeded}");
    block_direct(&estate, SOURCE, DEST, true);
    let open = stream(&estate, SOURCE, DEST, "after-failures");
    assert_eq!(open["kind"].as_str(), Some(format!("proxy via {CARRIER}").as_str()), "{open}");
    let failed = wait_for("the source's failed history is durable", Duration::from_secs(15), || async {
        let h = estate.connection_history(SOURCE);
        let n = rows(&h, DEST, "Direct").iter().filter(|r| r["state"] == "Failed").count();
        (n >= 7).then_some(n)
    })
    .await;

    // 2. The destination, a peer, opens toward the source through a carrier its topology holds.
    block_direct(&estate, DEST, SOURCE, true);
    let peer_open = stream(&estate, DEST, SOURCE, "peer-discovery");
    assert_eq!(peer_open["kind"].as_str(), Some(format!("proxy via {CARRIER}").as_str()), "the peer's carrier carried it on its own edge: {peer_open}");
    let peer = snapshot(&estate, DEST);
    let held: Vec<&Value> = peer["active_directs"].as_array().into_iter().flatten().collect();
    let foreign: Vec<&&Value> = held.iter().filter(|d| d["source"]["name"] != DEST).collect();
    assert!(foreign.is_empty(), "the peer holds no other node's fact, the carrier's edges and the source's failing pair ({failed} failed facts) included: {foreign:?}");
    assert!(held.iter().all(|d| d["kind"] == "Direct" && d["state"] == "Connected"), "the peer holds active Direct facts only: {held:?}");
    let proxies: Vec<&Value> = peer["own_active_proxies"].as_array().into_iter().flatten().collect();
    assert!(proxies.iter().all(|p| p["source"]["name"] == DEST), "the peer holds its own Proxy and no other node's: {proxies:?}");
    estate.stop().await;
}

/// CONTRACT (connections.md §13 F: "Start with active Proxy and failed Direct. Restore direct
/// connectivity. Assert: Direct Connected written, Proxy remains effective, Proxy
/// Disconnected(reason=direct-restored) written, Direct becomes effective"; §9.3, §9.5 steps 3-5; §10):
/// the source's open proved a Proxy after its blocked Direct dial (Failed round 1, ordinal 1); the
/// series' first due attempt fails under the block (ordinal 2). The block is released and nothing
/// else happens: no accept, no traffic. At the Failed row's due time the
/// source's own reconciler dials (`via-recovery-dial`, never earlier than `next_due`), and that dial's
/// Direct Connected is written. Its write passes; the Proxy's retirement write is refused, and an open
/// meanwhile still goes through the carrier. Released, `Proxy Disconnected(direct-restored)` lands, and
/// only then does an open go Direct. The dial is the source process's own (§15.11-12): no node-admin
/// emits a recovery dial. Must NOT happen: a Direct open before the retirement landed, a recovery
/// dial before the due time, or a Proxy Disconnected with another reason.
// @feature: connections-reconnect
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restored_direct_disconnects_the_proxy_before_the_next_open_goes_direct() {
    let cell = "a_restored_direct_disconnects_the_proxy_before_the_next_open_goes_direct";
    let mut estate = born(cell).await;
    carrier_edge(&estate).await;
    block_direct(&estate, SOURCE, DEST, true);
    let first = stream(&estate, SOURCE, DEST, "over-the-proxy");
    assert_eq!(first["kind"].as_str(), Some(format!("proxy via {CARRIER}").as_str()), "{first}");
    wait_for("the Proxy is held", Duration::from_secs(15), || async { (snapshot(&estate, SOURCE)["own_active_proxies"].as_array().map(Vec::len) == Some(1)).then_some(()) }).await;
    // The series' own first attempt, still blocked, fails at its due time: ordinal 2, same round. Its
    // backoff (8 s) is the window the fault is armed and the block released in.
    let failed_row = wait_for("the series' second attempt failed under the block", Duration::from_secs(20), || async {
        rows(&estate.connection_history(SOURCE), DEST, "Direct").into_iter().rev().find(|r| r["state"] == "Failed" && r["recovery"]["attempt_ordinal"] == 2).cloned()
    })
    .await;
    let failed: rdm_core::connections::NodeConnection = serde_json::from_value(failed_row.clone()).expect("a connections row decodes");
    let due_ms = rdm_core::reconnect::next_due(&failed).expect("a Failed row carries its due time");

    // 1. The retirement will be refused: the dial's Direct Connected append passes, every later one
    // is refused until released. Then the block is released.
    let armed = estate.probe(&["fault", "--target", &format!("path:{SOURCE}"), "--refuse-history", "100000", "--pass-history", "1", "--key", "x"]);
    assert_eq!(armed["fault"], "armed", "{armed}");
    block_direct(&estate, SOURCE, DEST, false);

    // 2. The series' own dial connects at its due time; the retirement is owed and refused.
    let owed = wait_for("the source's due dial connected and its retirement is owed", Duration::from_secs(30), || async {
        let snap = snapshot(&estate, SOURCE);
        let connected = snap["own_latest_directs"].as_array().into_iter().flatten().any(|d| d["destination"]["name"] == DEST && d["state"] == "Connected" && d["alpn"] == failed_row["alpn"]);
        (connected && snap["owed"].as_array().map(Vec::len) == Some(1) && snap["fault_refused"].as_u64().unwrap_or(0) >= 1).then_some(snap)
    })
    .await;
    assert_eq!(owed["own_active_proxies"].as_array().map(Vec::len), Some(1), "the Proxy stays held: {owed}");
    let during = stream(&estate, SOURCE, DEST, "while-owed");
    assert_eq!(during["kind"].as_str(), Some(format!("proxy via {CARRIER}").as_str()), "the Proxy stays effective until its retirement lands: {during}");

    // 3. Released: the retirement lands, then an open goes Direct.
    let released = estate.probe(&["fault", "--target", &format!("path:{SOURCE}"), "--release", "--key", "x"]);
    assert_eq!(released["fault"], "released", "{released}");
    wait_for("the retirement landed", Duration::from_secs(15), || async {
        let snap = snapshot(&estate, SOURCE);
        (snap["owed"].as_array().map(Vec::len) == Some(0) && snap["own_active_proxies"].as_array().map(Vec::len) == Some(0)).then_some(())
    })
    .await;
    let after = stream(&estate, SOURCE, DEST, "after-cutback");
    assert_eq!((after["kind"].as_str(), s(&after["answered"])), (Some("direct"), format!("echo {DEST}: after-cutback")), "{after}");

    // 4. The log, in order: Failed, Direct Connected, Proxy Disconnected(direct-restored).
    let history = estate.connection_history(SOURCE);
    let app: Vec<&Value> = history.iter().filter(|r| r["destination"]["name"] == DEST && r["alpn"] == failed_row["alpn"]).collect();
    let at = |pred: &dyn Fn(&Value) -> bool| app.iter().position(|r| pred(r));
    let connected = at(&|r| r["kind"] == "Direct" && r["state"] == "Connected").expect("Direct Connected written");
    let disconnected = at(&|r| r["kind"] == "Proxy" && r["state"] == "Disconnected").expect("Proxy Disconnected written");
    assert!(connected < disconnected, "Direct Connected precedes the Proxy's retirement: {app:?}");
    assert_eq!(app[disconnected]["reason"], "direct-restored", "{app:?}");
    estate.stop().await;

    // 5. Spans: the source's one recovery dial, at or after its due time, connected; no accept
    // woke it; the open while owed chose the Proxy before the landing.
    let spans = estate.spans();
    let dials: Vec<&Value> = named(&spans, "rdm.node_admin.connection.create.via-recovery-dial").into_iter().filter(|sp| sp["attributes"]["source"] == SOURCE && sp["attributes"]["destination"] == DEST && sp["attributes"]["outcome"] == "Reply").collect();
    assert_eq!(dials.len(), 1, "one connected recovery dial: {dials:#?}");
    let failed_dials = named(&spans, "rdm.node_admin.connection.create.via-recovery-dial").into_iter().filter(|sp| sp["attributes"]["source"] == SOURCE && sp["attributes"]["destination"] == DEST && sp["attributes"]["outcome"] != "Reply").count();
    assert!(failed_dials >= 1, "the blocked attempt is the series' own dial too");
    assert!(start(dials[0]) >= due_ms * 1_000_000, "the dial waited for the series' backoff: {} ns, due {due_ms} ms", start(dials[0]));
    assert!(s(&dials[0]["resource"]["service.name"]).is_empty() || s(&dials[0]["resource"]["service.name"]) == "rdm-rpc-node", "the dial is the source process's own: {}", dials[0]);
    let woken: Vec<&Value> = named(&spans, "rdm.node_admin.connection.update.via-accept-trigger").into_iter().filter(|sp| sp["attributes"]["source"] == SOURCE && sp["attributes"]["destination"] == DEST).collect();
    assert!(woken.is_empty(), "no accept woke the dial: {woken:#?}");
    let landed: Vec<&Value> = named(&spans, "rdm.node_admin.connection.update.via-retirement").into_iter().filter(|sp| sp["attributes"]["source"] == SOURCE && sp["attributes"]["outcome"] == "landed").collect();
    assert_eq!(landed.len(), 1, "{landed:#?}");
    estate.record_trace_url(&s(&dials[0]["trace_id"]));
}

/// CONTRACT (connections.md §13 J: "Drop the carrier's active Direct edge to destination. The carrier
/// refuses the next carried call with `carrier-edge-lost`; the source writes Proxy Disconnected with
/// that reason, stops selecting that Proxy, and either establishes another valid Proxy or exposes no
/// active route"; §8): the source holds a Proxy through the carrier. The carrier's own edge is lost:
/// its Direct toward the destination is deleted by id (`node.connection.delete { Connections([id]) }`,
/// its Disconnected written). The source's next open reaches the carrier,
/// which refuses the tunnel `carrier-edge-lost`; the source writes `Proxy Disconnected(carrier-edge-lost)`
/// and holds no Proxy. With no other carrier holding an edge, the next open has no route through a
/// carrier and ends by name. Must NOT happen: the stream carried, the Proxy still held, or another reason.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_carrier_that_lost_its_edge_refuses_by_name_and_the_source_disconnects_the_proxy() {
    let cell = "a_carrier_that_lost_its_edge_refuses_by_name_and_the_source_disconnects_the_proxy";
    let mut estate = born(cell).await;
    carrier_edge(&estate).await;
    block_direct(&estate, SOURCE, DEST, true);
    let first = stream(&estate, SOURCE, DEST, "over-the-proxy");
    assert_eq!(first["kind"].as_str(), Some(format!("proxy via {CARRIER}").as_str()), "{first}");
    wait_for("the Proxy is held", Duration::from_secs(15), || async { (snapshot(&estate, SOURCE)["own_active_proxies"].as_array().map(Vec::len) == Some(1)).then_some(()) }).await;

    // 1. The carrier loses its edge: its Direct toward the destination is closed by id, its
    // Disconnected landed and held by the carrier (a drop is no reconnect series: nothing re-dials it, §5.1).
    let edge = rows(&estate.connection_history(CARRIER), DEST, "Direct").into_iter().rev().find(|r| r["state"] == "Connected").cloned().expect("the carrier's edge");
    let closed = connections_delete_ids(&estate, CARRIER, &[s(&edge["id"])]).await;
    assert!(matches!(&closed, rdm_core::status::StatusReply::ConnectionsDeleted { closed, .. } if closed.iter().any(|c| c.to_string() == s(&edge["id"]))), "the carrier's edge is closed by id: {closed:?}");
    // The carrier refuses from what it holds (§8), and it holds the drop only once its write lands
    // (Q169); the drop's line is in the store file before its write lands (the write is then
    // fsynced), so the wait reads the carrier's held projection, never the file.
    wait_for("the carrier holds its Disconnected", Duration::from_secs(15), || async {
        snapshot(&estate, CARRIER)["own_latest_directs"].as_array().into_iter().flatten().any(|d| d["destination"]["name"] == DEST && d["alpn"] == edge["alpn"] && d["state"] == "Disconnected").then_some(())
    })
    .await;
    assert_eq!(rows(&estate.connection_history(CARRIER), DEST, "Direct").last().map(|r| r["state"].clone()), Some(serde_json::json!("Disconnected")), "the carrier's Disconnected is its latest Direct row");

    // 2. The source's next open: refused by the carrier by name; the Proxy is disconnected with it.
    let refused = stream(&estate, SOURCE, DEST, "after-edge-lost");
    assert_eq!(refused["streamed"].as_bool(), Some(false), "nothing is carried: {refused}");
    assert!(s(&refused["reason"]).contains("carrier-edge-lost"), "the carrier refused by name: {refused}");
    let dropped = wait_for("the source's Proxy Disconnected landed", Duration::from_secs(15), || async {
        rows(&estate.connection_history(SOURCE), DEST, "Proxy").last().filter(|r| r["state"] == "Disconnected").map(|r| (*r).clone())
    })
    .await;
    assert_eq!(dropped["reason"], "carrier-edge-lost", "{dropped}");
    assert_eq!(snapshot(&estate, SOURCE)["own_active_proxies"].as_array().map(Vec::len), Some(0), "the Proxy is no longer selected");
    // 3. No other carrier holds an edge: no route through a carrier.
    let none = stream(&estate, SOURCE, DEST, "no-route");
    assert_eq!(none["streamed"].as_bool(), Some(false), "no other valid Proxy exists: {none}");
    assert!(s(&none["reason"]).contains("NoActiveRoute"), "the source exposes no active route by name: {none}");
    estate.stop().await;
    let spans = estate.spans();
    let tunnels: Vec<&Value> = named(&spans, "rdm.node.connections.create.via-tunnel").into_iter().filter(|sp| sp["attributes"]["source"] == SOURCE && sp["attributes"]["carrier"] == CARRIER && sp["attributes"]["destination"] == DEST).collect();
    assert!(!tunnels.is_empty(), "{tunnels:#?}");
    estate.record_trace_url(&s(&tunnels.last().unwrap()["trace_id"]));
}

/// CONTRACT (connections.md §13 D: "Generate many failed reconnect attempts for one pair. Assert the
/// raw log contains the history while the current indexes contain at most one Direct member and one
/// Proxy member for that source/destination pair"; §3: "In RDM's file backend the raw log and the
/// current view are one file, `connections.jsonl`, the view the fold's latest line per key"; §15.5-6):
/// twelve Direct Failed observations of one pair through the source's own writer, then its Proxy. The
/// raw log holds all twelve, ordinals 1..=12 in one recovery round; the fold of the file holds one
/// Direct key (its latest line ordinal 12) and one Proxy key for the pair on that ALPN, and the
/// source's held projection one latest Direct for it. Must NOT happen: one current member per attempt.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_failed_attempts_grow_the_raw_log_and_hold_one_direct_and_one_proxy() {
    let cell = "many_failed_attempts_grow_the_raw_log_and_hold_one_direct_and_one_proxy";
    let mut estate = born(cell).await;
    let seeded = estate.probe(&["record-proxy", "--target", &format!("path:{SOURCE}"), "--destination", &format!("path:{DEST}"), "--carrier", &format!("path:{CARRIER}"), "--failed-attempts", "12", "--key", "x"]);
    assert_eq!(seeded["proxy_recorded"], true, "{seeded}");
    let control = rdm_core::connections::ALPN;
    let history = estate.connection_history(SOURCE);
    let failed: Vec<&Value> = rows(&history, DEST, "Direct").into_iter().filter(|r| r["state"] == "Failed" && r["alpn"] == serde_json::json!(control)).collect();
    assert_eq!(failed.iter().map(|r| r["recovery"]["attempt_ordinal"].as_u64().unwrap_or(0)).collect::<Vec<_>>(), (1..=12).collect::<Vec<u64>>(), "the raw log keeps every attempt: {failed:?}");
    assert!(failed.windows(2).all(|w| w[0]["recovery"]["recovery_round"] == w[1]["recovery"]["recovery_round"]), "one recovery round: {failed:?}");

    let file = estate.fabric_store_root().join(rdm_node::store_root::CONNECTIONS_FILE);
    let view = rdm_node::store_log::latest_in(&file).expect("the store's connections file reads whole");
    let pair = format!("{SOURCE}--{DEST}--{}--", hex::encode(control));
    let members: Vec<(&String, &rdm_node::store_log::Line)> = view.iter().filter(|((kind, key), _)| kind == "node-connection" && key.starts_with(&pair)).map(|((_, key), line)| (key, line)).collect();
    assert_eq!(members.iter().map(|(k, _)| k.trim_start_matches(&pair).to_string()).collect::<Vec<_>>(), vec!["direct".to_string(), "proxy".to_string()], "the current view holds one Direct and one Proxy member for the pair: {members:?}");
    let direct = &members[0].1.record;
    assert_eq!((direct["state"].as_str(), direct["recovery"]["attempt_ordinal"].as_u64()), (Some("Failed"), Some(12)), "the Direct member is the latest attempt: {direct}");
    let snap = snapshot(&estate, SOURCE);
    let held: Vec<&Value> = snap["own_latest_directs"].as_array().into_iter().flatten().filter(|d| d["destination"]["name"] == DEST && d["alpn"] == serde_json::json!(control)).collect();
    assert_eq!(held.len(), 1, "one latest Direct held for the pair: {snap}");
    assert_eq!(held[0]["recovery"]["attempt_ordinal"], 12, "{snap}");
    estate.stop().await;
}

/// A dialer the cell runs in its own process: an endpoint registering the app protocol, presenting
/// `cert` as `name` (a birth no node holds), its resolver holding only `target`'s birth. Returns its
/// registry, its router and its endpoint key as an acceptor's span names an unknown key (short form).
async fn foreign_dialer(name: &str, target: &Value, cert: Vec<u8>) -> (rdm_transport::connections::ConnectionsRegistry, iroh::protocol::Router, String) {
    let key = iroh::SecretKey::generate();
    let short = key.public().fmt_short().to_string();
    let ep = rdm_transport::endpoint::bind_with_alpns(key, "127.0.0.1:0".parse().unwrap(), &[rdm_test::node::APP_PROTOCOL.to_vec()]).await.expect("the dialer binds");
    let me = rdm_transport::connections::Birth { name: name.parse().unwrap(), node_id: rdm_core::NodeId::mint(), incarnation: rdm_core::IncarnationId::mint() };
    let resolver = std::sync::Arc::new(rdm_transport::StaticResolver::new());
    resolver.insert(rdm_transport::ResolvedNode {
        node_id: rdm_core::NodeId::parse(&s(&target["node_id"])).unwrap(),
        name: s(&target["name"]).parse().unwrap(),
        endpoint_id: s(&target["endpoint_id"]).parse().expect("the target's endpoint key"),
        transport_addr: s(&target["transport_addr"]).parse().expect("the target's transport address"),
        incarnation: rdm_core::IncarnationId::parse(&s(&target["incarnation_id"])).unwrap(),
    });
    let sink = std::sync::Arc::new(rdm_transport::connections::MemoryConnectionsSink::new(me));
    let (registry, handlers) = rdm_transport::connections::ConnectionsRegistry::new(ep.clone(), resolver, sink, &[rdm_test::node::app_protocol_proxy()], cert).expect("the dialer's registry");
    let mut rb = iroh::protocol::Router::builder(ep);
    for (alpn, handler) in handlers {
        rb = rb.accept(alpn, handler);
    }
    (registry, rb.spawn(), short)
}

/// Dial `target_name` presenting `cert` as `name`: the verdict's reason the dialer read, and the
/// acceptor's `via-refused` span for the dialer's key once exported. The acceptor stays
/// ready: nothing about the dialer is inferred beyond this one connection.
async fn refused_dial(estate: &Estate, name: &str, target_name: &str, cert: Vec<u8>) -> (String, Value) {
    let target = estate.node_value(target_name).await;
    let (registry, router, short) = foreign_dialer(name, &target, cert).await;
    let birth = rdm_transport::connections::Birth { name: target_name.parse().unwrap(), node_id: rdm_core::NodeId::parse(&s(&target["node_id"])).unwrap(), incarnation: rdm_core::IncarnationId::parse(&s(&target["incarnation_id"])).unwrap() };
    let reason = match registry.probe_direct(&birth, rdm_test::node::APP_PROTOCOL).await {
        Err(rdm_transport::connections::ConnectError::Refused { reason }) => reason,
        other => panic!("the acceptor refuses the dial by name: {other:?}"),
    };
    let _ = router.shutdown().await;
    let span = wait_for("the acceptor's refusal span is exported", Duration::from_secs(15), || async {
        named(&estate.spans(), "rdm.node.connections.reject.via-refused").into_iter().find(|sp| s(&sp["attributes"]["source"]) == short).cloned()
    })
    .await;
    let acceptor = estate.node_value(target_name).await;
    assert_eq!(s(&acceptor["status"]), "ready", "the refusal marks nothing on the acceptor: {acceptor}");
    (reason, span)
}

/// CONTRACT (node-connections-admit.md step 4 and Result: "`refused { reason }`: the verdict is written
/// back ... the connection is closed", Failure: "A dialer that writes no cert within the floor is
/// `no-credential`", Spans: "`rdm.node.connections.reject.via-refused` (the same, plus `reason`) on the
/// acceptor", Never: "no status set, no frame published, no mark on any node"; connections.md §12): a
/// dial on a live rpc node's registered app protocol presenting empty bytes is refused `no-credential`:
/// the dialer reads the verdict by name, the acceptor's span names the reason, its own path.name and
/// the alpn, and the acceptor stays ready. The dialer is a key the acceptor's topology view
/// does not name, so the span names its key and no `Refused` row is written for it (admit.rs `refuse`;
/// the row of a named dialer is `t:connections::a_refusal_is_a_refused_fact_on_the_acceptor_and_a_failed_row_on_the_dialer`).
/// Must NOT happen: the connection admitted or served, or another reason.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dial_presenting_no_member_cert_is_refused_no_credential_on_the_acceptor() {
    let cell = "a_dial_presenting_no_member_cert_is_refused_no_credential_on_the_acceptor";
    let mut estate = born(cell).await;
    let (reason, refused) = refused_dial(&estate, "mesh1.rpc.8", DEST, Vec::new()).await;
    assert_eq!(reason, "no-credential", "{refused}");
    assert_eq!((s(&refused["attributes"]["reason"]), s(&refused["attributes"]["destination"]), s(&refused["attributes"]["alpn"])), ("no-credential".to_string(), DEST.to_string(), String::from_utf8_lossy(rdm_test::node::APP_PROTOCOL).to_string()), "{refused}");
    estate.stop().await;
    let spans = estate.spans();
    let short = s(&refused["attributes"]["source"]);
    assert!(named(&spans, "rdm.node.connections.create.via-admitted").into_iter().all(|sp| s(&sp["attributes"]["source"]) != short && sp["attributes"]["source"] != "mesh1.rpc.8"), "the dial was never admitted");
    estate.record_trace_url(&s(&refused["trace_id"]));
}

/// CONTRACT (node-connections-admit.md Verification: "the issuer's signature against the fabric CA's
/// key and the CA's against a root of the node's set", Result `ca-bad-signature`; fabric-certs.md "On
/// a connection": "the acceptor reads the dialer's member cert ... verifies it ... and admits or drops
/// the connection"): a dial presenting a member cert correctly signed under another fabric's root (its
/// own CA and issuer) is refused `ca-bad-signature` by a live rpc node, whose root set is its launch's;
/// the dialer reads the reason and the acceptor's span names it. Must NOT happen: a cert of another
/// fabric admitted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dial_presenting_another_fabrics_cert_is_refused_ca_bad_signature_on_the_acceptor() {
    let cell = "a_dial_presenting_another_fabrics_cert_is_refused_ca_bad_signature_on_the_acceptor";
    let mut estate = born(cell).await;
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
    let root = rdm_core::certs::mint_key();
    let ca = rdm_core::certs::CaKey::mint(&root, rdm_core::certs::CA_CERT_TTL_SECS, now);
    let issuer = rdm_core::certs::IssuingKey::mint(&ca, "mesh1", now, now + 3_600_000).expect("within the CA's life");
    let birth = rdm_core::certs::BirthIdentity { node_id: rdm_core::NodeId::mint(), incarnation: rdm_core::IncarnationId::mint(), name: "mesh1.rpc.9".parse().unwrap(), mesh: "mesh1".into(), endpoint_key: rdm_core::EndpointId(iroh::SecretKey::generate().public().to_string()) };
    let cert = rdm_core::certs::cert_to_bytes(&rdm_core::certs::issue(&issuer, &birth, now, now + 600_000).expect("the foreign chain issues"));
    let (reason, refused) = refused_dial(&estate, "mesh1.rpc.9", DEST, cert).await;
    assert_eq!(reason, "ca-bad-signature", "{refused}");
    assert_eq!(s(&refused["attributes"]["reason"]), "ca-bad-signature", "{refused}");
    assert_ne!(s(&refused["attributes"]["cert_expires_at"]), "0", "the span names the presented cert's expiry: {refused}");
    estate.stop().await;
    estate.record_trace_url(&s(&refused["trace_id"]));
}

/// CONTRACT (node-connections-delete.md step 2: "`Connections([id, …])`: refuse an empty list by name.
/// Close exactly the named connections and queue each drop under the node's current name (Q169) ...
/// Return `node.connections.deleted` naming the ids closed, the ids unknown and the ids already closed.
/// The node's status does not change"; Wire: "`RejectedConnectionScope` for a scope that names nothing
/// the node can cut"): the source holds two open Direct connections on the app protocol, to the carrier
/// and to the destination, each listed by `node.connection.get` with its id. A delete naming the
/// carrier's id and an id the node never opened closes that one, names the unknown id not open and
/// leaves the other open; the drop is written under the source's name; the same id again is not open;
/// an empty list sent to the mesh-primary is refused by name; the source stays ready.
/// Must NOT happen: a connection not named closed, a named id ignored, or the status changed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_connections_delete_by_id_closes_that_connection_and_names_the_rest() {
    let cell = "a_connections_delete_by_id_closes_that_connection_and_names_the_rest";
    let mut estate = born(cell).await;
    for to in [CARRIER, DEST] {
        let out = stream(&estate, SOURCE, to, "open");
        assert_eq!(out["kind"].as_str(), Some("direct"), "{out}");
    }
    let listed = |view: &rdm_core::views::ConnectionsView, to: &str| -> Vec<String> { view.connections.iter().filter(|c| c.destination == to && c.kind == "direct" && c.state == "connected").map(|c| c.id.clone()).collect() };
    let birth = exact_birth(&estate, SOURCE).await;
    let before = wait_for("both open connections are listed", Duration::from_secs(15), || async {
        let v = estate.connections_obj().await.get(&birth, &rdm_client::Filter::new()).await.ok()?;
        (!listed(&v, CARRIER).is_empty() && !listed(&v, DEST).is_empty()).then_some(v)
    })
    .await;
    let (to_carrier, to_dest) = (listed(&before, CARRIER)[0].clone(), listed(&before, DEST)[0].clone());
    let never = rdm_core::ConnectionId::mint().to_string();

    let reply = connections_delete_ids(&estate, SOURCE, &[to_carrier.clone(), never.clone()]).await;
    let rdm_core::status::StatusReply::ConnectionsDeleted { closed, not_open, receipt, .. } = &reply else { panic!("node.connections.deleted: {reply:?}") };
    assert_eq!((closed.iter().map(ToString::to_string).collect::<Vec<_>>(), not_open.iter().map(ToString::to_string).collect::<Vec<_>>()), (vec![to_carrier.clone()], vec![never.clone()]), "{reply:?}");
    assert!(receipt.is_none(), "a scope of ids carries no node-wide receipt: {reply:?}");
    // The drop lands behind the answer (Q169); `node.connection.get` reads the held projection, which
    // takes the drop once it lands.
    let drop = wait_for("the drop is written under the source's name", Duration::from_secs(15), || async {
        rows(&estate.connection_history(SOURCE), CARRIER, "Direct").last().filter(|r| r["state"] == "Disconnected").map(|r| (*r).clone())
    })
    .await;
    let after = wait_for("the closed connection leaves node.connection.get", Duration::from_secs(15), || async {
        let v = estate.connections_obj().await.get(&birth, &rdm_client::Filter::new()).await.ok()?;
        (!listed(&v, CARRIER).contains(&to_carrier)).then_some(v)
    })
    .await;
    assert!(listed(&after, DEST).contains(&to_dest), "the connection not named stays open: {after:?}");
    assert_eq!(s(&drop["source"]["name"]), SOURCE, "{drop}");
    assert_ne!(s(&drop["id"]), to_carrier, "the drop is a new entry: {drop}");
    let again = connections_delete_ids(&estate, SOURCE, &[to_carrier.clone()]).await;
    assert!(matches!(&again, rdm_core::status::StatusReply::ConnectionsDeleted { closed, not_open, .. } if closed.is_empty() && not_open.iter().any(|c| c.to_string() == to_carrier)), "the same id again is not open: {again:?}");
    // An empty list, sent past the client's own refusal to the mesh-primary, is refused by name.
    let primary = mesh_primary(&estate).await;
    let carrier_client = estate.carrier().await;
    let empty = rdm_client::NodeRpc::new(carrier_client.client()).connections_delete(&primary, &rdm_transport::RouteChoice::Direct, &birth, &rdm_core::status::ConnectionScope::Connections(vec![]), &rdm_transport::CallOptions::default()).await.expect("answered");
    assert!(matches!(&empty, rdm_core::status::StatusReply::RejectedConnectionScope { reason } if reason.contains("at least one")), "an empty list is refused by name: {empty:?}");
    assert!(listed(&estate.connections_obj().await.get(&birth, &rdm_client::Filter::new()).await.expect("node.connection.get"), DEST).contains(&to_dest), "the refused scope cut nothing");
    let source = estate.node_value(SOURCE).await;
    assert_eq!(s(&source["status"]), "ready", "a delete of named connections changes no status: {source}");
    estate.stop().await;
}

/// The mesh1 primary's node id.
async fn mesh_primary(estate: &Estate) -> rdm_core::NodeId {
    let mesh = estate.nodes_obj().await.get(&rdm_client::Filter::new().eq("mesh", ["mesh1"])).await.expect("node.get [mesh]");
    mesh.iter().find(|n| n.is_primary).map(|n| n.node_id.clone()).expect("node.get names mesh1's primary")
}

/// `node`'s exact birth.
async fn exact_birth(estate: &Estate, node: &str) -> rdm_client::ExactBirth {
    let v = estate.node_view(node).await;
    rdm_client::ExactBirth { target: rdm_transport::NodeTarget::ExactNode(v.node_id.clone()), node_id: v.node_id.clone(), incarnation: v.incarnation_id.clone().expect("a live birth") }
}

/// CONTRACT (node-connections-delete.md step 3: "4. Close every open connection, each open Direct or
/// Proxy connection of the node. 5. Queue the drop rows under the node's current name"; "While the node
/// is connections-deleting the runtime refuses every new call except Status 0x1B, PrimaryUpdate 0x21
/// and NodeCommand 0x22 with typed `ConnectionsDeleting`"; connections.md §1.1 "R-CR5"): the source holds
/// one Direct (to the carrier) and one Proxy (to the destination, through the carrier) on the app
/// protocol. `node.connection.delete { All }` as a Build completes on `node.connections.deleted`;
/// afterwards `node.connection.get` (Status 0x1B) still answers and lists no open connection, both
/// drops are written under the source's name, and a new call to the source (the testkit door, op
/// 0x73) is refused `ConnectionsDeleting` by name. Must NOT happen: a connection still open after the
/// completion, a drop under another name, or a new call served.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_all_closes_every_data_plane_connection_and_the_node_refuses_new_work_by_name() {
    let cell = "an_all_closes_every_data_plane_connection_and_the_node_refuses_new_work_by_name";
    let mut estate = born(cell).await;
    carrier_edge(&estate).await;
    let direct = stream(&estate, SOURCE, CARRIER, "direct");
    assert_eq!(direct["kind"].as_str(), Some("direct"), "{direct}");
    block_direct(&estate, SOURCE, DEST, true);
    let proxy = stream(&estate, SOURCE, DEST, "proxy");
    assert_eq!(proxy["kind"].as_str(), Some(format!("proxy via {CARRIER}").as_str()), "{proxy}");
    let birth = exact_birth(&estate, SOURCE).await;
    let nodes = estate.nodes_obj().await;
    let open = |v: &rdm_core::views::ConnectionsView| -> Vec<(String, String, String)> { v.connections.iter().filter(|c| c.state == "connected").map(|c| (c.kind.clone(), c.destination.clone(), c.id.clone())).collect() };
    let before = wait_for("the Direct and the Proxy are listed open", Duration::from_secs(15), || async {
        let o = open(&estate.connections_obj().await.get(&birth, &rdm_client::Filter::new()).await.ok()?);
        (o.iter().any(|(k, d, _)| k == "direct" && d == CARRIER) && o.iter().any(|(k, d, _)| k == "proxy" && d == DEST)).then_some(o)
    })
    .await;

    let mut deleting = nodes.delete_connections(&SOURCE.parse().unwrap()).await.unwrap_or_else(|e| panic!("node.connection.delete of {SOURCE} refused: {e}"));
    let frames = Estate::frames_of(&mut deleting).await;
    assert_eq!(frames.last(), Some(&rdm_client::Frame::Complete), "{frames:?}");
    assert!(frames.contains(&rdm_client::Frame::Event(rdm_client::NodeEvent::ConnectionsDeleted)), "{frames:?}");

    let drops = wait_for("both drops are written", Duration::from_secs(15), || async {
        let h = estate.connection_history(SOURCE);
        let d: Vec<Value> = h.iter().filter(|r| r["state"] == "Disconnected" && ((r["kind"] == "Direct" && r["destination"]["name"] == CARRIER) || (r["kind"] == "Proxy" && r["destination"]["name"] == DEST))).cloned().collect();
        (d.len() >= 2).then_some(d)
    })
    .await;
    // Status 0x1B still answers; the held projection has taken both drops.
    let after = estate.connections_obj().await.get(&birth, &rdm_client::Filter::new()).await.expect("Status 0x1B still answers while connections are deleted");
    assert!(open(&after).is_empty(), "no connection is open once the drops landed: {after:?} (before: {before:?})");
    assert!(drops.iter().all(|r| r["source"]["name"] == SOURCE), "every drop under the source's name: {drops:?}");
    let refused = estate.probe(&["snapshot", "--target", &format!("path:{SOURCE}"), "--key", "x"]);
    assert!(s(&refused["refused"]).starts_with("ConnectionsDeleting"), "a new call is refused by name: {refused}");
    estate.stop().await;
}

/// CONTRACT (node-connections-delete.md step 0: "For `All` only: if the target is the fabric-primary or
/// a mesh-primary, it first runs node.primary.update ..., commanded from above; the Build continues
/// once node.primary.updated is answered, under the new primary"; Primary row): mesh2's primary P (not
/// the fabric-primary) is the target of a standalone `node.connection.delete`. The fabric-primary
/// commands P's node.primary.update (`primary.update.via-command`), P's election names the other
/// node-admin NA2 (`via-update-primary`, successor NA2), NA2 answers node.primary.updated; only then
/// does the `DeleteConnections` step run on P, executed by NA2 as mesh2's primary. The Build completes
/// on node.connections.deleted and every admin names NA2 mesh2's primary. Must NOT happen: a
/// `DeleteConnections` step before NA2 answered, or P still named primary.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mesh_primarys_connections_delete_hands_its_primary_over_before_its_all() {
    let cell = "a_mesh_primarys_connections_delete_hands_its_primary_over_before_its_all";
    let mut estate = Estate::bootstrap(owner(cell), "fabric1", "mesh1").await;
    estate.reconcile_shape("fabric1", &[("mesh1", 1, 1), ("mesh2", 2, 1)]).await;
    let views = estate.settled_views(&[("mesh1", 1, 1), ("mesh2", 2, 1)]).await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let p = views.iter().find(|n| n.mesh == "mesh2" && n.kind == rdm_core::NodeKind::NodeAdmin && n.is_primary).cloned().expect("mesh2's primary");
    let na2 = views.iter().find(|n| n.mesh == "mesh2" && n.kind == rdm_core::NodeKind::NodeAdmin && !n.is_primary).cloned().expect("mesh2's other node-admin");
    assert!(!p.is_fabric_primary, "mesh2's primary is not the fabric-primary");
    let (p_name, na2_name) = (p.name.to_string(), na2.name.to_string());

    let mut deleting = estate.nodes_obj().await.delete_connections(&p.name).await.unwrap_or_else(|e| panic!("node.connection.delete of {p_name} refused: {e}"));
    let frames = Estate::frames_of(&mut deleting).await;
    assert_eq!(frames.last(), Some(&rdm_client::Frame::Complete), "{frames:?}");
    for admin in views.iter().filter(|n| n.kind == rdm_core::NodeKind::NodeAdmin && n.name != p.name) {
        let view = estate.node_views_at(&admin.name).await.unwrap_or_else(|| panic!("{} answers node.get", admin.name));
        let primary: Vec<String> = view.iter().filter(|n| n.mesh == "mesh2" && n.kind == rdm_core::NodeKind::NodeAdmin && n.is_primary).map(|n| n.name.to_string()).collect();
        assert_eq!(primary, vec![na2_name.clone()], "{} names {na2_name} mesh2's primary", admin.name);
    }
    estate.stop().await;
    let spans = estate.spans();
    let one = |name: &str, f: &dyn Fn(&Value) -> bool| -> Value {
        let found: Vec<&Value> = named(&spans, name).into_iter().filter(|sp| f(sp)).collect();
        assert_eq!(found.len(), 1, "one {name}: {found:#?}");
        found[0].clone()
    };
    let cmd = one("rdm.node_admin.primary.update.via-command", &|sp| s(&sp["attributes"]["primary"]) == p_name);
    assert_eq!(s(&cmd["attributes"]["outcome"]), "updated", "{cmd:#?}");
    let upd = one("rdm.node_admin.primary.update.via-update-primary", &|sp| s(&sp["attributes"]["node"]) == p_name);
    assert_eq!((s(&upd["attributes"]["successor"]), s(&upd["attributes"]["outcome"])), (na2_name.clone(), "updated".to_string()), "{upd:#?}");
    let answered = one("rdm.node_admin.primary.update.via-primary-updated", &|sp| s(&sp["attributes"]["node"]) == na2_name && s(&sp["attributes"]["outcome"]) == "held");
    let step = named(&spans, "rdm.node_admin.deployment.update.via-step").into_iter().find(|sp| s(&sp["attributes"]["node"]) == p_name && s(&sp["attributes"]["step"]) == "DeleteConnections").cloned().expect("the DeleteConnections step on P");
    assert_eq!(s(&step["attributes"]["outcome"]), "complete", "{step}");
    assert!(start(&step) >= answered["end_unix_nano"].as_u64().unwrap_or(u64::MAX), "the All ran after NA2 answered node.primary.updated: {step} {answered}");
    let run = named(&spans, "rdm.node_admin.build.update.via-dispatch").into_iter().find(|sp| s(&sp["attributes"]["executor"]) == na2_name).unwrap_or_else(|| panic!("NA2 executed the delete"));
    assert!(cmd["end_unix_nano"].as_u64().unwrap_or(0) <= start(run), "the fabric-primary installed NA2's answer before it dispatched the delete");
    estate.record_trace_url(&s(&cmd["trace_id"]));
}

/// The carrier's own Direct to the destination's birth `dest_incarnation` on the app protocol, landed.
async fn carrier_edge_of(estate: &Estate, dest_incarnation: &str) {
    let edge = stream(estate, CARRIER, DEST, "carrier-edge");
    assert_eq!((edge["kind"].as_str(), s(&edge["peer_incarnation"]).as_str()), (Some("direct"), dest_incarnation), "the carrier reaches the destination's birth directly: {edge}");
    wait_for("the carrier's Direct Connected toward the destination's birth landed", Duration::from_secs(15), || async {
        rows(&estate.connection_history(CARRIER), DEST, "Direct").last().is_some_and(|r| r["state"] == "Connected" && s(&r["destination"]["incarnation"]) == dest_incarnation).then_some(())
    })
    .await;
}

/// Kill `node`'s runtime and wait for its mesh-primary's recovery: a new node, ready, at its path.name.
async fn recovered(estate: &Estate, node: &str) -> Value {
    let before = estate.node_value(node).await;
    estate.kill_node(node).await;
    estate
        .until_node(node, &format!("{node} is recovered as a new node at its path.name"), crate::node_recovery_due(), |n| {
            let n = serde_json::to_value(n?).expect("a view serializes");
            (n["status"] == "ready" && n["node_id"] != before["node_id"]).then_some(n)
        })
        .await
}

/// CONTRACT (connections.md §13 I: "A destination incarnation change invalidates the old Direct/Proxy
/// evidence for that process birth ... Stale evidence is never used for a new process birth"; §8:
/// "If any dependency is superseded, the source writes Proxy Disconnected with a named structural
/// reason ... `destination-incarnation-superseded`. The source may then select another currently
/// eligible carrier and establish a new Proxy"; §9.1 a series is for the current destination
/// incarnation): the source holds a Proxy to the destination's birth through the carrier. The
/// destination's runtime is killed and recovered as a new birth at its path.name, and the carrier
/// opens its own Direct to that birth. The source's next open writes `Proxy Disconnected
/// (destination-incarnation-superseded)` naming the old birth, starts a new series toward the new birth
/// (Direct Failed ordinal 1 naming it) and proves a new Proxy naming the new birth; the stream is
/// answered by the new birth. Must NOT happen: an open carried on the old Proxy, or a new fact naming
/// the old birth.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_proxy_naming_a_replaced_destination_birth_is_disconnected_by_name_and_never_carries() {
    let cell = "a_proxy_naming_a_replaced_destination_birth_is_disconnected_by_name_and_never_carries";
    let mut estate = born(cell).await;
    let old = estate.node_value(DEST).await;
    let old_inc = s(&old["incarnation_id"]);
    carrier_edge_of(&estate, &old_inc).await;
    block_direct(&estate, SOURCE, DEST, true);
    let first = stream(&estate, SOURCE, DEST, "old-birth");
    assert_eq!((first["kind"].as_str(), s(&first["peer_incarnation"])), (Some(format!("proxy via {CARRIER}").as_str()), old_inc.clone()), "{first}");
    wait_for("the Proxy is held", Duration::from_secs(15), || async { (snapshot(&estate, SOURCE)["own_active_proxies"].as_array().map(Vec::len) == Some(1)).then_some(()) }).await;

    let new = recovered(&estate, DEST).await;
    let new_inc = s(&new["incarnation_id"]);
    assert_ne!(new_inc, old_inc, "a new birth");
    for witness in [SOURCE, CARRIER] {
        estate.witness_holds_birth(witness, DEST).await;
    }
    carrier_edge_of(&estate, &new_inc).await;
    // What the source writes from here on: the old birth's series (its attempts under the block until
    // the kill) is history before this point.
    let before = estate.connection_history(SOURCE).len();

    let second = stream(&estate, SOURCE, DEST, "new-birth");
    assert_eq!((second["kind"].as_str(), s(&second["peer_incarnation"]), s(&second["answered"])), (Some(format!("proxy via {CARRIER}").as_str()), new_inc.clone(), format!("echo {DEST}: new-birth")), "the new birth answers over a new Proxy: {second}");
    let written = wait_for("the old Proxy's Disconnected and the new Proxy landed", Duration::from_secs(15), || async {
        let h = estate.connection_history(SOURCE);
        let after: Vec<Value> = h[before.min(h.len())..].to_vec();
        let dropped = after.iter().any(|r| r["kind"] == "Proxy" && r["state"] == "Disconnected");
        let proved = after.iter().any(|r| r["kind"] == "Proxy" && r["state"] == "Connected");
        (dropped && proved).then_some(after)
    })
    .await;
    let dropped = written.iter().find(|r| r["kind"] == "Proxy" && r["state"] == "Disconnected").unwrap();
    assert_eq!((s(&dropped["reason"]), s(&dropped["destination"]["incarnation"])), ("destination-incarnation-superseded".to_string(), old_inc.clone()), "{dropped}");
    let proved = written.iter().find(|r| r["kind"] == "Proxy" && r["state"] == "Connected").unwrap();
    assert_eq!(s(&proved["destination"]["incarnation"]), new_inc, "{proved}");
    let failed = written.iter().find(|r| r["kind"] == "Direct" && r["state"] == "Failed").expect("a new series toward the new birth");
    assert_eq!((s(&failed["destination"]["incarnation"]), failed["recovery"]["attempt_ordinal"].as_u64()), (new_inc.clone(), Some(1)), "{failed}");
    assert!(written.iter().filter(|r| r["state"] != "Disconnected").all(|r| s(&r["destination"]["incarnation"]) == new_inc), "no new fact names the old birth: {written:?}");
    estate.stop().await;
}

/// CONTRACT (connections.md §13 I: "A carrier incarnation change invalidates a Proxy that names the old
/// carrier process"; §8 `carrier-incarnation-superseded`; §5.1: a latest Failed Direct is
/// `NoActiveRoute`, its series owning the retry): the source holds a Proxy through the carrier's birth.
/// The carrier's runtime is killed and recovered as a new birth. The source's next open writes `Proxy
/// Disconnected(carrier-incarnation-superseded)` naming the old carrier birth and is carried by
/// nothing: its Direct toward the destination stands Failed, so it exposes no active route by name.
/// Must NOT happen: an open carried on the Proxy naming the old carrier birth.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_proxy_naming_a_replaced_carrier_birth_is_disconnected_by_name_and_never_carries() {
    let cell = "a_proxy_naming_a_replaced_carrier_birth_is_disconnected_by_name_and_never_carries";
    let mut estate = born(cell).await;
    let dest = estate.node_value(DEST).await;
    let old_carrier = s(&estate.node_value(CARRIER).await["incarnation_id"]);
    carrier_edge_of(&estate, &s(&dest["incarnation_id"])).await;
    block_direct(&estate, SOURCE, DEST, true);
    let first = stream(&estate, SOURCE, DEST, "old-carrier");
    assert_eq!(first["kind"].as_str(), Some(format!("proxy via {CARRIER}").as_str()), "{first}");
    wait_for("the Proxy is held", Duration::from_secs(15), || async { (snapshot(&estate, SOURCE)["own_active_proxies"].as_array().map(Vec::len) == Some(1)).then_some(()) }).await;

    let new = recovered(&estate, CARRIER).await;
    assert_ne!(s(&new["incarnation_id"]), old_carrier, "a new carrier birth");
    estate.witness_holds_birth(SOURCE, CARRIER).await;

    let second = stream(&estate, SOURCE, DEST, "after-carrier-replaced");
    assert_eq!(second["streamed"].as_bool(), Some(false), "nothing carries on the old carrier's Proxy: {second}");
    assert!(s(&second["reason"]).contains("NoActiveRoute"), "no active route, by name: {second}");
    let dropped = wait_for("the Proxy's Disconnected landed", Duration::from_secs(15), || async {
        rows(&estate.connection_history(SOURCE), DEST, "Proxy").last().filter(|r| r["state"] == "Disconnected").map(|r| (*r).clone())
    })
    .await;
    assert_eq!((s(&dropped["reason"]), s(&dropped["carrier"]["incarnation"])), ("carrier-incarnation-superseded".to_string(), old_carrier.clone()), "{dropped}");
    assert_eq!(snapshot(&estate, SOURCE)["own_active_proxies"].as_array().map(Vec::len), Some(0));
    estate.stop().await;
}
