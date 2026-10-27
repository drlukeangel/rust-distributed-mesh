//! Connections, end to end on the process provider: every acceptance test of
//! `docs/architecture/connections.md` §13, every invariant of §15 and every numbered step and
//! refusal of `lifecycles/node-connections-admit.md` and `lifecycles/node-connections-delete.md`,
//! mapped to the cell that proves it. A row's "any layer" column names the cells that prove it
//! below the process provider (unit, in-process transport, in-process node); its "process" column
//! names the process-provider cell (an estate of real node processes, `MESH_SPAWN_TYPE=process`).
//! A row whose process column is "none" is proved in this section, by the cell named in the last
//! column, or carries the reason it is not writable here.
//!
//! Crate keys: `t:` rdm-transport/tests, `n:` rdm-node/tests, `a:` rdm-admin/tests, `c:` rdm-core/tests,
//! `p:` rdm-test/tests (a path under it). `[gate]` marks a process cell no gate job runs today:
//! `p:rpc/proxy_hydration_cutback.rs` is `i143-2938-process` in tools/mesh-audit/i143-acceptance-jobs.json,
//! which scripts/rdm-gate-at.sh never starts, and the stem is in its `SCENARIO_SKIP`.
//!
//! # connections.md §13, required acceptance tests
//!
//! | test | any layer | process | this section |
//! |---|---|---|---|
//! | A one discovered proxy serves every consumer | `n:connection_reconnect::one_discovered_proxy_serves_every_consumer_of_the_same_destination`; `t:connections::a_held_proxy_is_taken_without_a_direct_dial`; `t:connections_integration::an_active_proxy_is_reused_without_a_fresh_direct_dial_ladder` | none | `one_discovered_proxy_carries_every_later_open_with_no_direct_dial` |
//! | B Proxy survives source restart | `n:connection_reconnect::a_proxy_survives_source_restart`; `a:connections_writer::a_restart_hydrates_the_latest_proxy_row_and_the_first_resolution_uses_it` | `p:rpc/proxy_hydration_cutback::source_restart_hydrates_proxy_routes_first_call_without_rediscovery` [gate] | - |
//! | C Failed reconnect backoff survives restart | `n:connection_reconnect::a_failed_reconnect_backoff_survives_restart`; `c:reconnect::after_a_restart_the_backoff_is_reconstructed_from_the_latest_failed_row_not_reset`; `a:connections_writer::a_restarted_birth_continues_the_failing_series_naming_its_own_birth` | none | `a_recovered_source_dials_no_earlier_than_its_failed_rows_due_time_at_the_next_ordinal` |
//! | D index stays bounded | `n:connection_reconnect::the_connections_index_stays_bounded_while_the_raw_log_keeps_history`; `a:connections_writer::many_failures_grow_the_raw_log_and_keep_the_index_at_one_direct_per_pair`; `a:connections_reconnect::source_reconnect_failures_grow_log_keep_two_current_members` | none | `many_failed_attempts_grow_the_raw_log_and_hold_one_direct_and_one_proxy` |
//! | E Failed/Dead stay out of fleet-wide held routing | `c:connections::a_disconnected_or_failed_direct_never_stays_resident_and_an_older_connect_never_revives_it`; `c:connections::another_nodes_proxy_is_never_held` | none | `a_peer_holds_none_of_a_sources_failed_history_or_its_proxy` |
//! | F reconnect success returns to Direct in order | `a:connections_writer::a_direct_connected_beside_a_proxy_disconnects_it_durably_and_the_route_cuts_back`; `t:connections::a_direct_connected_disconnects_the_proxy`; `t:connections_integration::a_direct_connected_owes_the_proxy_disconnected_write_and_new_calls_cut_back_only_after_it_is_durable`; `c:reconnect::cutback_to_direct_happens_only_after_the_proxy_disconnected_write_is_applied_and_a_failed_write_is_owed_again` | `p:rpc/proxy_hydration_cutback::source_accept_wakes_its_own_dial_whose_direct_disconnects_proxy_before_cutback` [gate] (its dial is woken by an accept, not by the series' due time) | `a_restored_direct_disconnects_the_proxy_before_the_next_open_goes_direct` |
//! | G an inbound accept is a recovery trigger | `a:connections_writer::an_accept_from_the_proxy_destination_writes_nothing_and_wakes_the_source_dial_whose_connected_disconnects_the_proxy`; `a:connections_writer::repeated_accepts_coalesce_into_one_attempt_that_waits_for_the_series_backoff`; `t:connection_observer::an_accepted_connection_is_handed_to_the_accept_trigger_once` | `p:rpc/proxy_hydration_cutback::source_accept_wakes_its_own_dial_whose_direct_disconnects_proxy_before_cutback` [gate] | - |
//! | H Proxy retirement write failure is safe and retried | `a:connections_writer::a_refused_disconnected_write_keeps_the_proxy_effective_until_it_lands`; `n:connection_writes::a_refused_connection_write_is_retried_with_backoff_until_it_lands` | `p:rpc/proxy_hydration_cutback::source_accept_wakes_its_own_dial_whose_direct_disconnects_proxy_before_cutback` [gate] | - |
//! | I destination/carrier incarnation | `c:connections::a_stale_destination_incarnation_is_superseded`; `c:connections::a_stale_carrier_incarnation_is_superseded`; `c:connections::a_direct_fact_naming_a_superseded_destination_is_not_current`; `t:connections_integration::a_fact_naming_a_superseded_birth_still_dials_the_current_one`; `t:connections::an_open_against_another_incarnation_is_wrong_incarnation_and_never_served`; `a:connections_writer::a_failed_dial_to_the_destinations_next_birth_starts_a_new_series_naming_that_birth` | none | `a_proxy_naming_a_replaced_destination_birth_is_disconnected_by_name_and_never_carries` |
//! | J carrier edge loss invalidates Proxy | `t:forward::a_carrier_whose_own_edge_to_the_target_is_not_active_refuses_carrier_edge_lost`; `t:forward::a_carried_call_answering_carrier_edge_lost_hands_the_proxy_back_to_be_disconnected`; `a:connections_writer::a_carrier_names_its_own_edge_only_when_its_latest_direct_is_not_connected` | none | `a_carrier_that_lost_its_edge_refuses_by_name_and_the_source_disconnects_the_proxy` |
//! | K sent request is never rerouted | `t:connections_integration::an_indeterminate_alone_never_invalidates_a_proxy`; `t:routing::certainty_stays_downstream_an_indeterminate_is_never_replayed_through_another_route`; `t:certainty::reply_loss_after_a_complete_send_is_indeterminate` | none | not writable here: no process door forces a post-commit answer timeout on a carried call (Decisions) |
//! | L #2640 independence | `t:routing::semantic_selection_is_not_connection_selection`; `t:routing::connection_changes_do_not_touch_the_selection_and_selection_changes_fabricate_no_connection_state` | none | n/a in RDM: no `orgs.topics`, route barrier or watermark exists here; the transport cells prove the selection is untouched |
//!
//! # connections.md §15, invariants
//!
//! | # | any layer | process | this section |
//! |---|---|---|---|
//! | 1 one durable history | `t:connections::a_direct_open_writes_connected_with_its_alpn_and_the_acceptor_sees_the_same_row_id` | `p:rpc/proxy_hydration_cutback` (its store root's log) [gate] | every cell reads the one log (`Estate::connection_history`) |
//! | 2 generic across node types | `c:connections::a_carrier_is_chosen_from_edges_naming_the_current_destination_process_spread_by_source` | `p:rpc/role_routing::a_gateway_reaches_the_selected_broker_direct_via_a_peer_gateway_or_not_at_all` | - |
//! | 3 Direct and Proxy kinds | `c:connections::malformed_entries_are_refused_by_name` | `p:rpc/proxy_hydration_cutback` [gate] | A, F |
//! | 4 state to virtual classification | none: RDM declares no virtual classification (§3.2); its file backend is one `connections.jsonl` (§3) | none | page-vs-code difference, reported |
//! | 5 raw log and one latest per kind | as D | none | D |
//! | 6 index never one member per retry | as D | none | D |
//! | 7 fleet-wide held state is active Direct only | as E | none | E |
//! | 8 active Proxy is source-owned | `c:connections::another_nodes_proxy_is_never_held` | none | E |
//! | 9 Failed/Dead read on demand, not held fleet-wide | `c:connections::a_disconnected_or_failed_direct_never_stays_resident_and_an_older_connect_never_revives_it` | none | E |
//! | 10 a valid Proxy overrides Direct until durably retired | `c:connections::the_route_prefers_a_valid_proxy_then_an_active_direct_then_none`; as F, H | `p:rpc/proxy_hydration_cutback::source_accept_wakes_its_own_dial_whose_direct_disconnects_proxy_before_cutback` [gate] | F |
//! | 11 every node repairs its own direct connections | `n:connection_reconnect::one_reconnect_attempt_at_a_time_per_node`; `a:connections_writer::a_failed_row_arms_the_due_time_reconciler_without_an_accept_reschedules_on_failure_and_coalesces_with_accepts` | none | C, F (the dial is the source process's own span) |
//! | 12 node-admin schedules no reconnect | none (an absence) | none | C, F (no node-admin process emits a recovery dial) |
//! | 13 not a platform Job | n/a: a structure, not a behaviour | - | - |
//! | 14 latest Failed row reconstructs the series | `c:reconnect::after_a_restart_the_backoff_is_reconstructed_from_the_latest_failed_row_not_reset`; `c:reconnect::a_new_incident_starts_a_new_round_at_ordinal_one` | none | C |
//! | 15 restart never resets backoff | as C | none | C |
//! | 16 a Direct Connected creates the retirement obligation | as F, G | `p:rpc/proxy_hydration_cutback` [gate] | F |
//! | 17 failed retirement writes are retried | as H | `p:rpc/proxy_hydration_cutback` [gate] | - |
//! | 18 incarnations make stale evidence refusable | as I | none | I |
//! | 19 a failed attempt is not node-death proof | `c:reconnect::route_resolution_never_creates_or_moves_a_reconnect` | none | C (the destination stays ready-for-traffic in node.get) |
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
//! | §1.2 `Refused` is the acceptor's fact | `t:connections::a_refusal_is_a_refused_fact_on_the_acceptor_and_a_failed_row_on_the_dialer` | none | `a_dial_presenting_no_member_cert_is_refused_no_credential_on_the_acceptors_log` |
//! | §2 a row carries its ALPN | `t:connections::a_direct_open_writes_connected_with_its_alpn_and_the_acceptor_sees_the_same_row_id` | none | A |
//! | §3.1 reachability is per protocol | `t:connections::a_working_alpn_a_proves_nothing_for_alpn_b_and_a_reconnect_on_a_never_disconnects_bs_proxy`; `n:connection_reconnect::alpn_interference_restores_a_while_b_keeps_its_proxy_from_the_shared_store` | none | - |
//! | §5.1 precedence; only a Failed latest waits for its series | `c:connections::the_route_prefers_a_valid_proxy_then_an_active_direct_then_none`; `c:connections::a_dropped_direct_is_dialled_again_and_only_a_failed_one_waits_for_its_series` | none | F |
//! | §5.2 a route change affects new invocations only | `t:connections::an_open_tunnel_and_its_streams_outlive_direct_restoration_and_the_next_open_goes_direct` | none | - |
//! | §6 carrier policy and deterministic spread | `t:connections::the_carrier_choice_is_kept_until_a_named_event_and_spread_by_source`; `t:connections::a_non_forwardable_alpn_never_takes_a_proxy_even_when_one_is_held`; `c:connections::a_valid_proxy_through_a_carrier_kind_the_protocol_forbids_is_skipped_not_disconnected` | none | - |
//! | §7 establishing a Proxy (Failed, carrier, tunnel `admitted`, Proxy Connected) | `t:connections::a_not_sent_direct_on_a_forwardable_alpn_goes_once_through_the_hashed_carrier`; `t:connections::the_tunnel_handshake_carries_the_destinations_admitted_at_the_exact_birth_and_only_then_writes_proxy_connected`; `t:connections::a_tunnel_refused_wrong_incarnation_or_lapsed_writes_nothing_and_ends_by_name` | none | A |
//! | §8 validity and named invalidation | as I, J | none | I, J |
//! | §9.4 one scheduler, one pending item | `n:connection_reconnect::one_reconnect_attempt_at_a_time_per_node` | none | - |
//! | §9.5 one attempt: verify birth, verify not superseded, dial, Failed or Connected | `a:connections_writer::an_accept_from_another_birth_arms_nothing_and_an_attempt_for_a_gone_birth_is_dropped_undialled`; `a:connections_writer::a_failed_dial_after_an_accept_keeps_the_proxy_and_reschedules_from_its_own_failed_row` | none | C, F |
//! | §10 a stream Proxy outlives its retirement | `t:connections::an_open_tunnel_and_its_streams_outlive_direct_restoration_and_the_next_open_goes_direct` | none | - |
//! | §12 transition and admission spans | `t:connections::a_dial_parents_to_the_callers_span_and_the_stream_bytes_are_not_spanned`; `t:connections::an_inbound_connection_on_a_registered_alpn_is_admitted_on_a_valid_cert`; `t:connections::a_refused_dial_is_spanned_with_the_presented_certs_expiry_and_the_clock_it_was_judged_on` | none | every cell asserts its spans by name |
//! | §12 `effective_route_cold_discovery_ms` | none: no code emits it | none | A (red first, then the attribute) |
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
//! | step 4 `refused { reason }`, `Refused` fact, closed | `t:connections::a_refusal_is_a_refused_fact_on_the_acceptor_and_a_failed_row_on_the_dialer` | none | `a_dial_presenting_no_member_cert_is_refused_no_credential_on_the_acceptors_log` |
//! | step 5 the dialer reads the verdict; its reconnect rule follows | `n:connection_reconnect::a_refused_dial_starts_a_series_a_refused_attempt_continues_and_an_admitted_one_ends` | none | - |
//! | `no-credential` | `t:connections::a_dial_that_presents_nothing_within_the_floor_is_no_credential`; `a:app_protocol_dials::an_admins_app_protocol_refuses_a_dialer_holding_no_cert_or_an_expired_one_by_name` | none | `a_dial_presenting_no_member_cert_is_refused_no_credential_on_the_acceptors_log` |
//! | `ca-bad-signature` | `t:connections::an_inbound_connection_is_refused_by_name_on_ca_bad_signature` | none | `a_dial_presenting_another_fabrics_cert_is_refused_ca_bad_signature_on_the_acceptors_log` |
//! | `ca-expired` | `t:connections::an_inbound_connection_is_refused_by_name_on_ca_expired` | none | - (one process refusal of each class proves the acceptor's path; the chain checks are the transport cells') |
//! | `issuer-bad-signature` | `t:connections::an_inbound_connection_is_refused_by_name_on_issuer_bad_signature` | none | - (as `ca-expired`) |
//! | `issuer-expired` | `t:connections::an_inbound_connection_is_refused_by_name_on_issuer_expired` | none | - (as `ca-expired`) |
//! | `issuer-mesh-mismatch` | `t:connections::an_inbound_connection_is_refused_by_name_on_issuer_mesh_mismatch` | none | - (as `ca-expired`) |
//! | `bad-signature` | `t:connections::an_inbound_connection_is_refused_by_name_on_bad_signature` | none | - (as `ca-expired`) |
//! | `expired` | `t:connections::an_inbound_connection_is_refused_by_name_on_expired`; `a:app_protocol_dials::an_admins_app_protocol_refuses_a_dialer_holding_no_cert_or_an_expired_one_by_name` | none | - (as `ca-expired`) |
//! | Never: nothing inferred about the dialer | none | none | the refusal cells: the refused dialer's path.name stays ready-for-traffic |
//! | `NoCerts` admits by name | `t:connections::a_no_certs_acceptor_admits_by_name`; `p:member_cert::a_mesh_primary_configured_with_no_certs_admits_the_birth_with_an_empty_cert_and_says_so` | none | - |
//! | a read the acceptor cannot complete writes nothing | `t:connections::a_dial_that_presents_nothing_within_the_floor_is_no_credential` (its reset arm) | none | - |
//! | spans `via-admitted`, `via-refused`, `via-presented` | `t:connections::an_inbound_connection_on_a_registered_alpn_is_admitted_on_a_valid_cert`; `t:connections::a_refused_dial_is_spanned_with_the_presented_certs_expiry_and_the_clock_it_was_judged_on` | none | the refusal cells |
//!
//! # lifecycles/node-connections-delete.md
//!
//! | step / refusal | any layer | process | this section |
//! |---|---|---|---|
//! | step 0 a primary target runs node.primary.update first (`All`) | inside a replace: `p:elections/node_primary_update::a_planned_hand_over_makes_the_same_mesh_successor_both_primaries_on_its_direct_answer`; inside a stop: `p:node/node_stop::a_stop_of_a_mesh_primary_hands_over_by_node_primary_update_before_it_stops` | none for a standalone `node.connections.delete` | `a_mesh_primarys_connections_delete_hands_its_primary_over_before_its_all` |
//! | step 1 the exact birth, the scope | `p:node/node_connections_delete::a_standalone_connections_delete_completes_on_connections_deleted_and_leaves_the_process_running` | as any layer | - |
//! | step 2 `Connections([…])`: exactly those closed, unknown and already-closed named | `p:connections_delete::a_connections_delete_naming_exact_ids_cuts_only_those_and_names_every_id` | `p:node/node_connections_delete::a_mesh_primary_named_in_its_own_connections_delete_runs_its_own_command_path_and_records_from_its_own_frames` (an id never opened) | `a_connections_delete_by_id_closes_that_connection_and_names_the_rest` |
//! | step 2 an empty list refused by name | `p:connections_delete::a_connections_delete_naming_exact_ids_cuts_only_those_and_names_every_id`; `rdm-core status.rs` unit | none | `a_connections_delete_by_id_closes_that_connection_and_names_the_rest` |
//! | step 3.1 block, report `ConnectionsDeleting`, then the record | `p:command_natural_key::the_node_answers_a_connections_delete_on_its_command_stream_in_order_for_both_scopes` | `p:node/node_connections_delete::a_connections_delete_is_admitted_by_the_node_on_its_command_stream_and_nothing_is_recorded_before_the_send` | - |
//! | steps 3.2-3.3 admitted work finishes | `p:connections_all::all_closes_the_connections_opened_while_it_waited_for_admitted_work` | none | - |
//! | step 3.4 every open Direct or Proxy closed | `p:connections_all::a_standalone_all_closes_the_log_connections_and_leaves_the_node_heard_on_gossip`; `p:connections_delete::an_all_whose_drops_cannot_land_answers_at_once_and_closes_every_live_connection` | none | `an_all_closes_every_data_plane_connection_and_the_node_refuses_new_work_by_name` |
//! | step 3.5 drops under the current name | `p:connections_rename::a_renamed_nodes_next_connections_entries_name_the_name_it_holds_at_both_ends` | none | `an_all_closes_every_data_plane_connection_and_the_node_refuses_new_work_by_name` |
//! | step 3.6 `Established` and the closed ids | `p:command_natural_key::the_node_answers_a_connections_delete_on_its_command_stream_in_order_for_both_scopes` | `p:node/node_connections_delete::a_connections_delete_is_admitted_by_the_node_on_its_command_stream_and_nothing_is_recorded_before_the_send` | - |
//! | step 4 connections-deleted set from `Established` | none: no node status `connections-deleted` exists (`rdm-core` views.rs `NodeStatus`) | none | page-vs-code difference, reported |
//! | step 5 `Deadline` at `RDM_CONNECTIONS_DELETE_DEADLINE_MS` | `p:command_natural_key::an_all_with_work_that_never_finishes_ends_at_its_bound_with_a_deadline_receipt` | none | - |
//! | step 6 nothing inferred: no stop, exit or node.deleted | `p:node/node_connections_delete::a_standalone_connections_delete_completes_on_connections_deleted_and_leaves_the_process_running` | as any layer | - |
//! | connections-deleting refuses all but 0x1B, 0x21, 0x22 | `a:status_rpc::connections_deleting_mesh_primary_serves_status_checks_exact_birth`; `a:admin_serves_forward::a_connections_deleting_admin_refuses_forward_by_name`; `t:forward::a_nested_forward_and_a_carrier_deleting_connections_are_still_refused_by_name` | none | `an_all_closes_every_data_plane_connection_and_the_node_refuses_new_work_by_name` |
//! | the process runs; gossip still heard | `p:connections_all::a_standalone_all_closes_the_log_connections_and_leaves_the_node_heard_on_gossip` | `p:node/node_connections_delete::a_standalone_connections_delete_completes_on_connections_deleted_and_leaves_the_process_running` | - |
