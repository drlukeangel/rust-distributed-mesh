# Test name census (rafka-v2 #3208)

Every RDM test under `crates/` and `tools/` checked against `docs/eng/testing-naming-conventions.md` (landed 23bb09f97). Report only: nothing is renamed. Produced by a script over every `#[test]` and `#[tokio::test]` attribute (including the cells the `cells!`, `successor_cells!` and `rows!` macros generate), file and line being the `fn` line (the macro invocation line for a macro cell).

## Totals

- Tests checked: 2299 (906 unit cells in `src/`, 1393 in `tests/`).
- Conforming to every mechanically checkable rule: 2165. With at least one finding: 134.
- Findings by rule: 1.1: 17; 1.2: 30; 1.3: 88; 2.4: 3; file names (Rule 2.5): 9.
- Rule 1.4 (line 45, "Most RDM cells carry a `/// CONTRACT:`"): 1444 cells carry one, 855 do not. The doc states "most", not every, so these are not findings and are not tabled.
- Rule 2.1 (line 63): 51 test files sit at the top of `crates/rdm-test/tests/` outside a family; the doc says "Some files still sit" there, so they are not findings.
- Rule 3.2: the gate adds the suffix skip itself (`scripts/rdm-gate-at.sh:286`), so no finding. Rules 2.2, 3.1, 3.3 and 3.4 are file-structure and landing rules not decidable per cell name; not checked per cell.

## What was and was not checked

- Rule 1.1 is a sentence rule; only its checkable floor is tabled: a name of fewer than four words cannot hold an actor, an action and an outcome, and a name ending `_works`, `_test`, `_basic`, `_smoke` or `_sanity` names a label, not an assertion. A name that passes may still not read as a checkable sentence; that is the reviewer's call.
- Rule 1.3 uses CLAUDE.md's full ban list (which exceeds the name check's `RETIRED` set: it adds `epoch`, `holder`, `tree`, `partition`, `resolver`, `cut-off`, `held-running`, `hung hold`, `committed seat`, `exit proof`, `proven exit`, `child mesh`, `leadership view`), matched on whole `_`-separated words and their inflections. `holder` has a page sense in `NameHeld`, `round` matches `round_trip`, `partition` matches network-partition cells: the hit is listed, the reviewer rules each.
- Rule 1.2 is applied literally as the doc states it: a `_full_` or `_test_` inside a name is listed even where the word has its plain meaning (`a_full_channel_queue…`, `a_sigkilled_test_binary…`); the reviewer rules each.
- Name-check refusals (`scripts/name-check.py`) apply only to what a diff adds, so every Rule 1.3 / 2.5 row below is a name that predates the check.

## Findings: file names

| file | rule broken (doc line) |
|---|---|
| `crates/rdm-admin/tests/pipeline_moves_no_seat.rs` | Rule 2.5 (line 79): "No retired word in a file name": banned word in file name: seat |
| `crates/rdm-admin/tests/status_seat_fold.rs` | Rule 2.5 (line 79): "No retired word in a file name": banned word in file name: seat |
| `crates/rdm-test/tests/elections/cohort_election.rs` | Rule 2.5 (line 79): "No retired word in a file name": banned word in file name: cohort |
| `crates/rdm-test/tests/elections/late_authority.rs` | Rule 2.5 (line 79): "No retired word in a file name": banned word in file name: authority |
| `crates/rdm-test/tests/elections/role_cohort.rs` | Rule 2.5 (line 79): "No retired word in a file name": banned word in file name: cohort |
| `crates/rdm-test/tests/gossip/partition_rejoin.rs` | Rule 2.5 (line 79): "No retired word in a file name": banned word in file name: partition |
| `crates/rdm-test/tests/mesh/cohort_recovery.rs` | Rule 2.5 (line 79): "No retired word in a file name": banned word in file name: cohort |
| `crates/rdm-test/tests/rpc/proof_holder_lifecycle.rs` | Rule 2.5 (line 79): "No retired word in a file name": banned word in file name: holder |
| `crates/rdm-transport/tests/seat_frames_wire.rs` | Rule 2.5 (line 79): "No retired word in a file name": banned word in file name: seat |

## Findings: cells

| test name | file:line | rule broken (doc line) |
|---|---|---|
| `a_create_mesh_refused_by_name_is_receipted_failed_naming_the_holder` | `crates/rdm-admin/src/admin.rs:1119` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `holder` |
| `a_drift_lanes_removal_hold_closes_on_its_own_attempts_verdict_and_on_no_other_lanes` | `crates/rdm-admin/src/admin.rs:1919` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `lanes` |
| `a_mesh_primary_announces_its_meshs_cohort_seats_the_election_fills` | `crates/rdm-admin/src/admin.rs:2740` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `cohort`, `seats` |
| `the_fold_reads_the_fabric_lane_and_each_drift_lane_apart` | `crates/rdm-admin/src/build_state.rs:3678` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `lane` |
| `a_fact_line_written_before_lanes_decodes_as_the_fabric_lane` | `crates/rdm-admin/src/build_state.rs:3714` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `lane`, `lanes` |
| `port_holder_names_this_process_for_its_own_listener_and_no_socket_for_a_free_port` | `crates/rdm-admin/src/deployment/endpoint.rs:321` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `holder` |
| `a_silent_birth_is_held_until_its_exact_runtime_is_proven_exited` | `crates/rdm-admin/src/drift.rs:105` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `proven_exit` |
| `a_node_type_short_without_a_proven_exit_is_not_drift` | `crates/rdm-admin/src/drift.rs:135` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `proven_exit` |
| `a_moved_fabric_seat_record_refuses_the_old_seats_decisions_with_no_fence_write` | `crates/rdm-admin/src/fabric_builds.rs:417` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `fence`, `seat`, `seats` |
| `a_seat_commit_waits_for_the_decision_in_flight_and_the_moved_record_refuses_every_later_one` | `crates/rdm-admin/src/fabric_builds.rs:453` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `seat` |
| `a_second_fabric_of_a_held_name_under_one_store_dir_is_refused_naming_the_holder` | `crates/rdm-admin/src/fabric_storage.rs:352` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `holder` |
| `an_admin_investigates_a_silent_fabric_holder_from_its_own_view` | `crates/rdm-admin/src/seat_watch.rs:392` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `holder` |
| `an_admin_investigates_neither_a_ready_holder_nor_its_own_birth` | `crates/rdm-admin/src/seat_watch.rs:403` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `holder` |
| `a_claim_put_to_an_admin_that_is_not_its_claimant_names_who_may_claim_and_a_mesh_primary_claims_its_own_drift_lane` | `crates/rdm-admin/tests/build_claim.rs:225` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `lane` |
| `a_fabric_primary_whose_seat_record_moved_claims_and_dispatches_nothing` | `crates/rdm-admin/tests/build_drive.rs:183` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `seat` |
| `a_fabric_primary_whose_view_shows_another_holder_names_the_seat_records_holder_never_the_views` | `crates/rdm-admin/tests/build_drive.rs:233` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `holder`, `seat` |
| `a_fenced_fabric_primary_is_refused_fenced_whatever_its_view_shows_and_names_the_new_seat` | `crates/rdm-admin/tests/build_drive.rs:267` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `fenced`, `seat` |
| `a_fence_set_by_a_committed_transfer_names_the_successor_while_the_view_still_shows_the_old_seat` | `crates/rdm-admin/tests/build_drive.rs:286` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `fence`, `seat` |
| `a_seat_row_naming_another_holder_than_the_executor_ends_the_drive_naming_that_holder` | `crates/rdm-admin/tests/build_drive.rs:1058` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `holder`, `seat` |
| `a_seat_row_naming_the_executor_ends_the_drive_on_the_executors_new_seat_frame_never_before` | `crates/rdm-admin/tests/build_drive.rs:1115` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `seat` |
| `a_holder_lost_after_its_row_is_proven_departed_before_its_successor_acknowledges_the_row_and_dispatches` | `crates/rdm-admin/tests/build_drive.rs:2027` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `departed`, `holder` |
| `an_incomplete_responder_is_passed_over_and_only_full_holdings_count` | `crates/rdm-admin/tests/build_facts_read.rs:428` | Rule 1.2 (line 34): "No `_test_`, `_smoke_`, `_critical_path_`, `_coverage_` or `_full_` in a cell name": label fragment `full` |
| `exit_proofs_of_a_peer_mesh_open_nothing_before_the_ladders_decision_or_after_the_mesh_is_heard` | `crates/rdm-admin/tests/drift_convergence.rs:1660` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `exit_proof` |
| `every_gossip_frame_and_build_message_round_trips_through_postcard_under_the_ceiling` | `crates/rdm-admin/tests/gossip_frames_wire.rs:324` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `round` |
| `a_join_request_and_its_answer_round_trip_through_postcard` | `crates/rdm-admin/tests/join_wire.rs:28` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `round` |
| `a_delete_cut_after_its_exit_proof_is_finished_by_the_resumed_attempt_from_its_missing_steps` | `crates/rdm-admin/tests/mesh_delete_resume.rs:1838` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `exit_proof` |
| `a_handoff_held_ends_the_owners_wait_with_its_manifest_before_a_later_proven_exit` | `crates/rdm-admin/tests/mesh_delete_resume.rs:2033` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `proven_exit` |
| `a_node_created_with_a_free_text_name_round_trips_name_path_and_node_type_through_create_get_stop_start_recovery_and_replace` | `crates/rdm-admin/tests/node_rows.rs:1676` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `round` |
| `a_recovery_whose_mesh_primary_departed_mid_run_is_ended_by_the_new_mesh_primary_on_its_exit_proof` | `crates/rdm-admin/tests/recovery_admission.rs:660` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `departed`, `exit_proof` |
| `an_in_flight_row_a_departed_mesh_primary_left_after_its_verdict_is_released_by_the_new_mesh_primary_on_its_exit_proof` | `crates/rdm-admin/tests/recovery_admission.rs:760` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `departed`, `exit_proof` |
| `a_stopped_nodes_delete_goes_straight_to_the_exact_runtimes_exit_proof` | `crates/rdm-admin/tests/stopped_delete.rs:98` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `exit_proof` |
| `every_reply_variant_encodes_and_classifies_the_same_after_a_round_trip` | `crates/rdm-core/src/build_claim.rs:227` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `round` |
| `encode_decode_roundtrip` | `crates/rdm-core/src/certs.rs:702` | Rule 1.1 (line 20): `<actor>_<action>_<expected_outcome>()`; line 31: "The outcome is the assertion, never the operation's name": name is 3 word(s): no actor, action and outcome sentence |
| `valid_cert_accepted` | `crates/rdm-core/src/certs.rs:713` | Rule 1.1 (line 20): `<actor>_<action>_<expected_outcome>()`; line 31: "The outcome is the assertion, never the operation's name": name is 3 word(s): no actor, action and outcome sentence |
| `wrong_root_rejected` | `crates/rdm-core/src/certs.rs:721` | Rule 1.1 (line 20): `<actor>_<action>_<expected_outcome>()`; line 31: "The outcome is the assertion, never the operation's name": name is 3 word(s): no actor, action and outcome sentence |
| `expired_cert_rejected` | `crates/rdm-core/src/certs.rs:730` | Rule 1.1 (line 20): `<actor>_<action>_<expected_outcome>()`; line 31: "The outcome is the assertion, never the operation's name": name is 3 word(s): no actor, action and outcome sentence |
| `a_well_formed_context_is_kept_whole_and_round_trips` | `crates/rdm-core/src/context.rs:158` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `round` |
| `a_birth_round_trips_as_json_for_gossip` | `crates/rdm-core/src/digest.rs:176` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `round` |
| `every_reply_variant_round_trips` | `crates/rdm-core/src/forward.rs:378` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `round` |
| `varint_round_trips_at_every_width` | `crates/rdm-core/src/framing.rs:493` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `round` |
| `request_round_trips` | `crates/rdm-core/src/framing.rs:509` | Rule 1.1 (line 20): `<actor>_<action>_<expected_outcome>()`; line 31: "The outcome is the assertion, never the operation's name": name is 3 word(s): no actor, action and outcome sentence |
| `request_round_trips` | `crates/rdm-core/src/framing.rs:509` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `round` |
| `reply_frames_round_trip_and_name_their_failures` | `crates/rdm-core/src/framing.rs:671` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `round` |
| `every_reply_variant_encodes_and_classifies_the_same_after_a_round_trip` | `crates/rdm-core/src/join.rs:228` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `round` |
| `the_custody_document_round_trips_every_value_and_refuses_a_malformed_one_by_name` | `crates/rdm-core/src/launch.rs:485` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `round` |
| `launch_round_trips_through_the_environment` | `crates/rdm-core/src/launch.rs:514` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `round` |
| `an_op_round_trips_and_its_key_is_the_operation_not_a_step` | `crates/rdm-core/src/lifecycle.rs:83` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `round` |
| `path_name_round_trips` | `crates/rdm-core/src/model.rs:252` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `round` |
| `every_node_command_frame_round_trips_under_its_bounds` | `crates/rdm-core/src/node_command.rs:513` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `round` |
| `path_name_round_trips_and_refuses_by_name` | `crates/rdm-core/src/path.rs:235` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `round` |
| `request_codec_round_trips_through_the_frame` | `crates/rdm-core/src/ping.rs:128` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `round` |
| `every_shared_constructor_classifies_the_same_after_a_round_trip` | `crates/rdm-core/src/ping.rs:141` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `round` |
| `a_record_round_trips_through_the_data_dir` | `crates/rdm-core/src/runtime.rs:373` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `round` |
| `every_reply_variant_encodes_and_classifies_the_same_after_a_round_trip` | `crates/rdm-core/src/status.rs:1833` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `round` |
| `a_full_chunk_fits_the_reply_frame_ceiling` | `crates/rdm-core/tests/build_facts_wire.rs:74` | Rule 1.2 (line 34): "No `_test_`, `_smoke_`, `_critical_path_`, `_coverage_` or `_full_` in a cell name": label fragment `full` |
| `a_create_spec_is_the_entry_before_q166_with_its_config_appended` | `crates/rdm-core/tests/build_wire.rs:413` | Rule 2.4 (line 73): "No issue id in a test file name" (applied to a cell name): issue or question id in the name |
| `a_join_request_and_its_digest_round_trip_through_postcard_with_every_field_present` | `crates/rdm-core/tests/join_wire.rs:44` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `round` |
| `a_join_request_round_trips_no_listeners_and_three` | `crates/rdm-core/tests/join_wire.rs:94` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `round` |
| `resolver_unknown_is_not_found_and_gone_is_unavailable_with_its_typed_detail` | `crates/rdm-core/tests/outcome_codes.rs:220` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `resolver` |
| `a_new_incident_starts_a_new_round_at_ordinal_one` | `crates/rdm-core/tests/reconnect.rs:73` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `round` |
| `a_snapshot_chunk_round_trips_with_its_digests_and_overlays` | `crates/rdm-core/tests/topology_wire.rs:140` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `round` |
| `a_remover_waits_on_the_names_mutex_and_proceeds_when_its_holder_dies` | `crates/rdm-node/src/store_log/tests.rs:574` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `holder` |
| `every_reply_variant_encodes_and_round_trips` | `crates/rdm-test/src/declare_probe.rs:204` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `round` |
| `the_reaper_of_a_killed_test_binary_kills_a_node_whose_only_mark_is_its_data_dir` | `crates/rdm-test/src/estate.rs:1489` | Rule 1.2 (line 34): "No `_test_`, `_smoke_`, `_critical_path_`, `_coverage_` or `_full_` in a cell name": label fragment `test` |
| `an_arm_of_every_cut_kind_round_trips_on_the_wire` | `crates/rdm-test/src/fault_door.rs:427` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `round` |
| `every_reply_variant_encodes_and_round_trips` | `crates/rdm-test/src/fault_door.rs:441` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `round` |
| `a_partition_drops_each_direction_by_both_ports_and_an_empty_side_cuts_nothing` | `crates/rdm-test/src/netfault.rs:165` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `partition` |
| `the_protocol_is_testkit_forwardable_and_every_variant_round_trips` | `crates/rdm-test/src/proof_store.rs:666` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `round` |
| `every_reply_variant_encodes_and_round_trips` | `crates/rdm-test/src/resolve_probe.rs:247` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `round` |
| `container_network_partition_of_a_peer_mesh_heals_with_one_live_birth_per_path` | `crates/rdm-test/tests/chaos/container_fault_backend.rs:261` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `partition` |
| `partition_subset_cuts_a_node_set_from_the_rest_until_healed` | `crates/rdm-test/tests/chaos/restored_faults.rs:167` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `partition` |
| `partitioned_meshes_rejoin_with_neighbors_preserve_live_births` | `crates/rdm-test/tests/gossip/partition_rejoin.rs:149` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `partitioned` |
| `a_real_topology_read_then_gossip_of_every_category_rolls_nothing_back` | `crates/rdm-test/tests/gossip/recovery_receive.rs:34` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `rolls` |
| `a_leave_whose_primary_is_lost_after_a_members_exit_proof_finishes_that_members_delete_under_the_surviving_admin` | `crates/rdm-test/tests/mesh/mesh_delete_resume.rs:341` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `exit_proof` |
| `a_failed_build_for_a_proven_exit_is_submitted_again_by_the_admin_that_takes_over_mesh_primary` | `crates/rdm-test/tests/node/node_replace.rs:735` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `proven_exit` |
| `a_replace_rolled_back_by_its_recipe_ends_with_the_predecessor_ready_under_its_name` | `crates/rdm-test/tests/node/node_replace.rs:1178` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `rolled` |
| `poc_probe_round_trips_and_a_cut_frame_is_not_sent` | `crates/rdm-test/tests/poc_chaos_kit.rs:142` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `round` |
| `proof_holder_applies_cas_reports_exact_executing_birth` | `crates/rdm-test/tests/proof_store.rs:97` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `holder` |
| `a_start_whose_commander_the_resolver_holds_deleted_joins_it_over_its_call` | `crates/rdm-test/tests/rejoin_commander.rs:349` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `resolver` |
| `the_product_family_round_trips_to_a_broker_and_is_unserved_at_a_compute` | `crates/rdm-test/tests/role_process.rs:154` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `round` |
| `proof_holder_restarts_preserves_same_node_store` | `crates/rdm-test/tests/rpc/proof_holder_lifecycle.rs:84` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `holder` |
| `mock_partition_heal_recovers_coverage_and_routes` | `crates/rdm-test/tests/rshape_burn_in.rs:5546` | Rule 1.2 (line 34): "No `_test_`, `_smoke_`, `_critical_path_`, `_coverage_` or `_full_` in a cell name": label fragment `coverage` |
| `mock_partition_heal_recovers_coverage_and_routes` | `crates/rdm-test/tests/rshape_burn_in.rs:5546` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `partition` |
| `mock_soak_partition_preserves_continuous_invariants` | `crates/rdm-test/tests/rshape_burn_in.rs:6987` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `partition` |
| `a_sigkilled_test_binary_leaves_no_process_or_directory_of_its_process_estate` | `crates/rdm-test/tests/runtime/container_kill.rs:255` | Rule 1.2 (line 34): "No `_test_`, `_smoke_`, `_critical_path_`, `_coverage_` or `_full_` in a cell name": label fragment `test` |
| `a_sigkilled_test_binary_leaves_no_container_network_or_directory_of_its_container_estate` | `crates/rdm-test/tests/runtime/container_kill.rs:266` | Rule 1.2 (line 34): "No `_test_`, `_smoke_`, `_critical_path_`, `_coverage_` or `_full_` in a cell name": label fragment `test` |
| `scenario_manifest_round_trips_owner_seed_and_fault_schedule` | `crates/rdm-test/tests/runtime/manifest_replay.rs:433` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `round` |
| `a_partition_and_a_rejoin_leave_the_clock_continuous_and_re_converge` | `crates/rdm-transport/src/clock.rs:694` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `partition` |
| `a_refused_forward_is_named_and_the_next_publication_owes_a_full` | `crates/rdm-transport/src/forward_path.rs:484` | Rule 1.2 (line 34): "No `_test_`, `_smoke_`, `_critical_path_`, `_coverage_` or `_full_` in a cell name": label fragment `full` |
| `a_queue_with_no_sender_task_is_named_and_owes_a_full` | `crates/rdm-transport/src/forward_path.rs:513` | Rule 1.2 (line 34): "No `_test_`, `_smoke_`, `_critical_path_`, `_coverage_` or `_full_` in a cell name": label fragment `full` |
| `a_chunked_full_is_sent_whole_before_the_decision_after_it` | `crates/rdm-transport/src/forward_path.rs:653` | Rule 1.2 (line 34): "No `_test_`, `_smoke_`, `_critical_path_`, `_coverage_` or `_full_` in a cell name": label fragment `full` |
| `round_trip` | `crates/rdm-transport/src/framer.rs:111` | Rule 1.1 (line 20): `<actor>_<action>_<expected_outcome>()`; line 31: "The outcome is the assertion, never the operation's name": name is 2 word(s): no actor, action and outcome sentence |
| `round_trip` | `crates/rdm-transport/src/framer.rs:111` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `round` |
| `truncation_detected` | `crates/rdm-transport/src/framer.rs:124` | Rule 1.1 (line 20): `<actor>_<action>_<expected_outcome>()`; line 31: "The outcome is the assertion, never the operation's name": name is 2 word(s): no actor, action and outcome sentence |
| `empty_input_rejected` | `crates/rdm-transport/src/framer.rs:137` | Rule 1.1 (line 20): `<actor>_<action>_<expected_outcome>()`; line 31: "The outcome is the assertion, never the operation's name": name is 3 word(s): no actor, action and outcome sentence |
| `legacy_tag_constant` | `crates/rdm-transport/src/framer.rs:150` | Rule 1.1 (line 20): `<actor>_<action>_<expected_outcome>()`; line 31: "The outcome is the assertion, never the operation's name": name is 3 word(s): no actor, action and outcome sentence |
| `a_full_channel_queue_drops_the_newest_by_name_and_the_carrier_keeps_receiving` | `crates/rdm-transport/src/gossip.rs:919` | Rule 1.2 (line 34): "No `_test_`, `_smoke_`, `_critical_path_`, `_coverage_` or `_full_` in a cell name": label fragment `full` |
| `traced_frame_round_trip_ping` | `crates/rdm-transport/src/ops.rs:103` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `round` |
| `traced_frame_round_trip_hello` | `crates/rdm-transport/src/ops.rs:127` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `round` |
| `a_departure_ends_every_overlay_of_its_birth_so_the_receiver_holds_the_publishers_full` | `crates/rdm-transport/src/snapshot.rs:986` | Rule 1.2 (line 34): "No `_test_`, `_smoke_`, `_critical_path_`, `_coverage_` or `_full_` in a cell name": label fragment `full` |
| `an_exit_flush_on_a_full_stderr_pipe_nobody_reads_ends_at_its_bound` | `crates/rdm-transport/src/telemetry.rs:843` | Rule 1.2 (line 34): "No `_test_`, `_smoke_`, `_critical_path_`, `_coverage_` or `_full_` in a cell name": label fragment `full` |
| `a_newer_version_of_the_held_revision_published_before_its_publisher_stopped_is_taken_and_one_stamped_before_the_successors_full_is_ignored` | `crates/rdm-transport/src/topology.rs:7364` | Rule 1.2 (line 34): "No `_test_`, `_smoke_`, `_critical_path_`, `_coverage_` or `_full_` in a cell name": label fragment `full` |
| `a_new_revision_on_the_mesh_channel_is_installed_with_no_primary_record_and_the_lost_births_earlier_full_is_ignored` | `crates/rdm-transport/src/topology.rs:7433` | Rule 1.2 (line 34): "No `_test_`, `_smoke_`, `_critical_path_`, `_coverage_` or `_full_` in a cell name": label fragment `full` |
| `an_earlier_full_delta_or_read_of_the_held_publisher_is_ignored_by_the_receive_path_before_the_snapshot_receiver_reads_it` | `crates/rdm-transport/src/topology.rs:7467` | Rule 1.2 (line 34): "No `_test_`, `_smoke_`, `_critical_path_`, `_coverage_` or `_full_` in a cell name": label fragment `full` |
| `admission_full_is_an_immediate_typed_busy_and_the_handler_never_runs` | `crates/rdm-transport/tests/certainty.rs:162` | Rule 1.2 (line 34): "No `_test_`, `_smoke_`, `_critical_path_`, `_coverage_` or `_full_` in a cell name": label fragment `full` |
| `outage_child` | `crates/rdm-transport/tests/collector_outage.rs:22` | Rule 1.1 (line 20): `<actor>_<action>_<expected_outcome>()`; line 31: "The outcome is the assertion, never the operation's name": name is 2 word(s): no actor, action and outcome sentence |
| `a_proven_exit_ends_the_owed_request_and_silence_does_not` | `crates/rdm-transport/tests/delivery.rs:618` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `proven_exit` |
| `an_owed_call_ends_on_its_receivers_proven_exit_unavailable` | `crates/rdm-transport/tests/delivery.rs:1328` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `proven_exit` |
| `a_flood_of_32_callers_for_10_seconds_round_trips_without_error` | `crates/rdm-transport/tests/flood.rs:79` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `round` |
| `exit_child` | `crates/rdm-transport/tests/telemetry_exit.rs:95` | Rule 1.1 (line 20): `<actor>_<action>_<expected_outcome>()`; line 31: "The outcome is the assertion, never the operation's name": name is 2 word(s): no actor, action and outcome sentence |
| `init_child` | `crates/rdm-transport/tests/telemetry_init.rs:41` | Rule 1.1 (line 20): `<actor>_<action>_<expected_outcome>()`; line 31: "The outcome is the assertion, never the operation's name": name is 2 word(s): no actor, action and outcome sentence |
| `two_runs_over_one_tree_write_the_same_bytes` | `tools/docgen/src/lib.rs:241` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `tree` |
| `a_job_whose_cell_matches_no_test_is_refused_by_name` | `tools/mesh-audit/tests/acceptance_gate.rs:74` | Rule 1.2 (line 34): "No `_test_`, `_smoke_`, `_critical_path_`, `_coverage_` or `_full_` in a cell name": label fragment `test` |
| `the_n0_preset_with_relay_disabled_is_an_n0_lookup` | `tools/mesh-audit/tests/address_lookup.rs:35` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `relay` |
| `the_rdm_tree_configures_no_n0_lookup_and_keeps_canonical_crates_off_the_legacy_plane` | `tools/mesh-audit/tests/address_lookup.rs:74` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `tree` |
| `test_code_may_define_and_accept_any_alpn` | `tools/mesh-audit/tests/address_lookup.rs:112` | Rule 1.2 (line 34): "No `_test_`, `_smoke_`, `_critical_path_`, `_coverage_` or `_full_` in a cell name": label fragment `test` |
| `test_code_may_define_and_accept_any_alpn` | `tools/mesh-audit/tests/address_lookup.rs:112` | Rule 1.2 (line 34): "No `_test_`, `_smoke_`, `_critical_path_`, `_coverage_` or `_full_` in a cell name": `test` label as the first word |
| `unmapped_clause_fails` | `tools/mesh-audit/tests/connections_parity.rs:40` | Rule 1.1 (line 20): `<actor>_<action>_<expected_outcome>()`; line 31: "The outcome is the assertion, never the operation's name": name is 3 word(s): no actor, action and outcome sentence |
| `pending_row_owned_by_something_other_than_an_i143_story_fails` | `tools/mesh-audit/tests/connections_parity.rs:53` | Rule 2.4 (line 73): "No issue id in a test file name" (applied to a cell name): issue or question id in the name |
| `duplicate_clause_fails` | `tools/mesh-audit/tests/connections_parity.rs:67` | Rule 1.1 (line 20): `<actor>_<action>_<expected_outcome>()`; line 31: "The outcome is the assertion, never the operation's name": name is 3 word(s): no actor, action and outcome sentence |
| `mirror_row_with_rdm_sha_and_test_passes` | `tools/mesh-audit/tests/connections_parity.rs:87` | Rule 1.2 (line 34): "No `_test_`, `_smoke_`, `_critical_path_`, `_coverage_` or `_full_` in a cell name": label fragment `test` |
| `rafka_domain_row_needs_a_named_owner_but_not_an_i143_story` | `tools/mesh-audit/tests/connections_parity.rs:94` | Rule 2.4 (line 73): "No issue id in a test file name" (applied to a cell name): issue or question id in the name |
| `rules_hold_on_the_rdm_tree` | `tools/mesh-audit/tests/dependency_rules.rs:65` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `tree` |
| `clean_fixture_passes` | `tools/mesh-audit/tests/dependency_rules.rs:71` | Rule 1.1 (line 20): `<actor>_<action>_<expected_outcome>()`; line 31: "The outcome is the assertion, never the operation's name": name is 3 word(s): no actor, action and outcome sentence |
| `rule5_binary_that_enables_test_knobs_in_dependencies_fails` | `tools/mesh-audit/tests/dependency_rules.rs:197` | Rule 1.2 (line 34): "No `_test_`, `_smoke_`, `_critical_path_`, `_coverage_` or `_full_` in a cell name": label fragment `test` |
| `rule5_binary_that_enables_test_knobs_from_dev_dependencies_only_passes` | `tools/mesh-audit/tests/dependency_rules.rs:203` | Rule 1.2 (line 34): "No `_test_`, `_smoke_`, `_critical_path_`, `_coverage_` or `_full_` in a cell name": label fragment `test` |
| `rule5_test_knobs_is_a_dev_dependency_feature_of_the_scenario_crate_alone` | `tools/mesh-audit/tests/dependency_rules.rs:212` | Rule 1.2 (line 34): "No `_test_`, `_smoke_`, `_critical_path_`, `_coverage_` or `_full_` in a cell name": label fragment `test` |
| `export_manifest_refuses_a_candidate_whose_fast_or_release_gate_log_is_missing_red_or_at_another_tree` | `tools/mesh-audit/tests/export_certifier.rs:508` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `tree` |
| `evidence_preservation_runs_after_the_concurrent_test_barrier` | `tools/mesh-audit/tests/gate_evidence.rs:11` | Rule 1.2 (line 34): "No `_test_`, `_smoke_`, `_critical_path_`, `_coverage_` or `_full_` in a cell name": label fragment `test` |
| `the_rdm_tree_is_green_under_every_ratchet` | `tools/mesh-audit/tests/lock_ratchets.rs:72` | Rule 1.3 (line 38): "CLAUDE.md's vocabulary bans hold in every name": banned word `tree` |
| `no_json_on_the_wire_reads_only_non_test_code_and_holds_its_proof_cell` | `tools/mesh-audit/tests/lock_ratchets.rs:348` | Rule 1.2 (line 34): "No `_test_`, `_smoke_`, `_critical_path_`, `_coverage_` or `_full_` in a cell name": label fragment `test` |
| `no_span_guard_across_await_accepts_scoped_dropped_discarded_and_test_guards` | `tools/mesh-audit/tests/lock_ratchets.rs:383` | Rule 1.2 (line 34): "No `_test_`, `_smoke_`, `_critical_path_`, `_coverage_` or `_full_` in a cell name": label fragment `test` |
| `one_rename_write_path_reads_only_non_test_code` | `tools/mesh-audit/tests/lock_ratchets.rs:426` | Rule 1.2 (line 34): "No `_test_`, `_smoke_`, `_critical_path_`, `_coverage_` or `_full_` in a cell name": label fragment `test` |
| `duplicate_row_fails` | `tools/mesh-audit/tests/parity_scanner.rs:142` | Rule 1.1 (line 20): `<actor>_<action>_<expected_outcome>()`; line 31: "The outcome is the assertion, never the operation's name": name is 3 word(s): no actor, action and outcome sentence |
| `unknown_sha_fails` | `tools/mesh-audit/tests/parity_scanner.rs:171` | Rule 1.1 (line 20): `<actor>_<action>_<expected_outcome>()`; line 31: "The outcome is the assertion, never the operation's name": name is 3 word(s): no actor, action and outcome sentence |
| `registry_command_that_is_not_cargo_test_is_refused_by_name` | `tools/test-runner/src/lib.rs:1491` | Rule 1.2 (line 34): "No `_test_`, `_smoke_`, `_critical_path_`, `_coverage_` or `_full_` in a cell name": label fragment `test` |
| `libtest_log_gives_each_test_its_own_status_and_panic` | `tools/test-runner/src/lib.rs:1497` | Rule 1.2 (line 34): "No `_test_`, `_smoke_`, `_critical_path_`, `_coverage_` or `_full_` in a cell name": label fragment `test` |
| `a_torn_result_line_is_a_pass_when_the_summary_counts_every_test_and_no_failure` | `tools/test-runner/src/lib.rs:1508` | Rule 1.2 (line 34): "No `_test_`, `_smoke_`, `_critical_path_`, `_coverage_` or `_full_` in a cell name": label fragment `test` |
