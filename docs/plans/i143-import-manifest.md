# i143 import manifest

**Export SHA:** `abc1ec1570c983f419fcb3b43e6adbe8b5712ce8` (RDM `main`, 2026-10-20), the revision rafka-v2 `Cargo.toml` pins once (i141). **Parity-through:** rafka-v2 `2a02378db468247fd2785af32605de5876294f0a` (base `87045580`, ledger `sha256:48b134a2…`, 0 MIRROR pending).
The six imported packages, at the export revision in rafka-v2 `Cargo.toml` and `Cargo.lock`:
`rdm-core`, `rdm-transport`, `rdm-node`, `rdm-admin`, `rdm-client`, `rdm-test`.
Forks: `iroh` / `iroh-base` at `drlukeangel/iroh` `23ab5a84`, `iroh-gossip` at `drlukeangel/iroh-gossip` `508fb795` (RDM `Cargo.toml:51-53`; the back-out of the read-only diag lines Luke permitted, Q75/Q77; `dce7b706` is its ancestor); each fork's divergence and its removal condition are stated beside its pin (`Cargo.toml:33-50`).

## Gate record at the export SHA

`export-manifest --candidate abc1ec1570c983f419fcb3b43e6adbe8b5712ce8 --repo /home/admin/rafka-v2` (2026-10-20), `eligible=true`: 77 owners, 77 receipts certified, 2 gate logs, 81 §14 cells proven, 0 refusals; the manifest it wrote is committed as [i143-export-manifest.json](i143-export-manifest.json) (each receipt and gate log with its sha256).

| gate | result | evidence |
|---|---|---|
| parity scan (`parity-scan --through <parity-through>`) | eligible through `2a02378db4`, 0 MIRROR pending | `i143-export-manifest.json` (`parity_ledger_digest`, `mirror_pending`) |
| fast gate (test cadence: staleness 3 s, gossip 500 ms) | green, 73 jobs ok, 0 FAIL, DONE 185 s | `/home/admin/rust-distributed-mesh-deploy/gates-e4s8-abc1ec15.log` (`[gate] head=abc1ec15… dirty=0`) |
| release gate (the release-only jobs, each stem at its own cadence) | green, 110 jobs ok, 0 FAIL, DONE 499 s | `/home/admin/rust-distributed-mesh-deploy/gates-e4s8-abc1ec15release.log` (`[gate] head=abc1ec15… dirty=0`) |
| 30-minute seeded soak, all eight process faults incl. fabric-primary loss and admin wedge | 12 of 12 ok (`i143-2787-soak-{process,container}`, `i143-rshape-soak-{authority,lifecycle,partition,randomized,routing}-{process,container}`), 5,847 s in three batches; the ten story soaks each wrote `violations: []` with every invariant holding (`invariant-results.json`: 30 to 350 rounds a story) | the receipts `target/i143-rshape/jobs/<job>.json`, each hashed in `i143-export-manifest.json`; PRD e11.s12 |
| container proof (PRD §12.2 cells, Docker domain: `crates/rdm-admin/tests/container_proof.rs`, `T/runtime/container_kill.rs`) | green: the release's `container-proof` job (29 s) and the 16 container receipts of the export run | `gates-e4s8-abc1ec15release.log`; `target/i143-acceptance/jobs/*-container.json`, `target/i143-rshape/jobs/*-container.json` |

## §14 export gate: every bullet and the cell that proves it

Every bullet is the PRD's current §14 (`docs/plans/i143-prd.md` §14), each proven by a cell on RDM
`main`, each resolving to one test of the gate's discovery. Paths are RDM's: `T/` = `crates/rdm-test/tests/`, `A/` =
`crates/rdm-admin/`. `export-manifest` reads every cell of this section and takes the fast and release
gate logs at the export SHA as their proof (`tools/mesh-audit/src/export.rs`).

### Identity

| bullet | proving cell |
|---|---|
| NodeId/MeshId/FabricId are canonical Crockford60 typed logical ids | `T/mesh/canonical_ids.rs::every_product_identity_is_canonical_and_kept_or_reminted_by_its_lifecycle`; `crates/rdm-core/tests/crockford60_parity.rs::every_rafka_minted_id_is_a_node_mesh_and_fabric_id_as_is` |
| EndpointId is a distinct transport identity | `T/mesh/canonical_ids.rs::every_product_identity_is_canonical_and_kept_or_reminted_by_its_lifecycle` |
| no canonical 32-char hex logical product id remains | `crates/rdm-core/src/ids.rs::tests::deserializing_refuses_a_non_canonical_product_id` |
| a live mesh keeps its MeshId; a mesh created again under a dead mesh's name gets a new MeshId | `T/mesh/peer_mesh_investigation.rs::a_peer_mesh_without_its_admins_gets_a_node_admin_only_on_a_carrier_edge_lost_verdict_after_two_probes` (the node-admin created into the live mesh joins under its MeshId); `T/mesh/canonical_ids.rs::every_product_identity_is_canonical_and_kept_or_reminted_by_its_lifecycle` (a mesh removed and created again gets a new MeshId); `A/tests/store_root_boot.rs::an_admin_started_on_a_prepared_root_reads_its_mesh_row_by_its_mesh_id_and_never_joins_a_mesh_created_again_under_its_name` |
| restart preserves NodeId and incarnation; node recovery and replacement mint a new NodeId; nothing alive comes back under the same fabric id and MeshIds | `T/mesh/canonical_ids.rs::every_product_identity_is_canonical_and_kept_or_reminted_by_its_lifecycle` (restart keeps NodeId and incarnation; a node removed and created again at its path gets a new NodeId); `A/tests/node_rows.rs::a_recovered_nodes_row_is_deleted_and_the_new_node_at_its_path_name_claims_it_seeded` |

### Accepted topology / Build

| bullet | proving cell |
|---|---|
| the current fabric-primary is the only topology writer; non-primary topology mutation is refused by name | `A/tests/node_objects.rs::a_build_call_to_an_admin_that_is_not_fabric_primary_is_refused_under_its_own_reject_span_naming_the_fabric_primary` (`not-fabric-primary`) |
| each Build is its own entity, written once in acceptance order; the current Build is the first in progress or completed, read last in, first out; no pointer to the newest Build | `A/src/build_state.rs::tests::the_current_build_is_the_newest_in_progress_or_completed_and_a_failed_one_is_skipped`; `A/tests/current_build.rs::an_accepted_build_is_current_from_the_build_store_alone` |
| a Build is durable before any reader can read it as current | `A/tests/current_build.rs::an_accepted_build_is_current_from_the_build_store_alone` |
| every admin reads the Fabric record and the current Build before authority-bearing Ready | `T/elections/late_lower_node_id_admin.rs::a_lower_node_id_admin_born_after_a_build_wins_and_manages_its_births`; `A/src/admin.rs::tests::an_admin_without_a_current_build_can_never_take_over` |
| older Build history is not replayed to reconstruct topology | `T/build/accepted_topology.rs::the_current_build_read_from_the_build_facts_is_the_one_accepted_topology` |
| every change to the shape (node.create, node.delete, the replace steps, mesh.delete, fabric.delete) is its own Build compiled from the current topology plus its change | `A/src/accepted.rs::tests::a_submitted_change_compiles_to_exact_paths_and_a_shrink_keeps_the_primary`; `A/src/accepted.rs::tests::mesh_changes_compile_and_the_refusals_are_named`; `A/src/accepted.rs::tests::a_build_plans_its_steps_in_order_and_never_reruns_a_completed_one`; `A/tests/current_build.rs::a_change_after_a_failed_build_compiles_against_the_newest_completed_build`; `A/src/accepted.rs::tests::a_fabric_delete_compiles_to_the_build_its_page_names` |
| a second topology-changing request while the current Build is in progress is refused `BuildInProgress { current_build_id }` | `A/tests/build_admission.rs::two_concurrent_topology_submissions_admit_one_build_and_refuse_the_other_naming_it` |
| authority movement, restart, node recovery and proven drift use another attempt of the current Build | `T/build/accepted_topology.rs::the_current_build_read_from_the_build_facts_is_the_one_accepted_topology` (restart, node recovery: the next attempt of the same Build) |
| an attempt lost with its executor leaves the Build in progress; only a named terminal failure fails it, and a failed Build is skipped | `A/tests/build_drive.rs::a_leave_that_ends_incomplete_on_a_lost_primary_leaves_the_build_in_progress_and_resumes_when_the_loss_is_proven`; `T/build/build_op.rs::a_lost_executor_ends_the_inner_call_indeterminate_and_the_successor_is_claimed_only_after_proof`; `A/tests/build_drive.rs::a_leave_that_ends_with_a_named_failure_fails_the_build_and_the_read_skips_it`; `A/src/build_state.rs::tests::the_current_build_is_the_newest_in_progress_or_completed_and_a_failed_one_is_skipped` |
| `build.delete` refuses the current Build by name | `T/build/accepted_topology.rs::the_current_build_read_from_the_build_facts_is_the_one_accepted_topology` (`cannot-delete`) |
| `fabric.get` answers the current Build | `T/build/accepted_topology.rs::the_current_build_read_from_the_build_facts_is_the_one_accepted_topology` |
| all-admin restart restores the current Fabric record and the current Build from RDM storage | `A/src/fabric_storage.rs::tests::the_file_storage_reloads_what_it_wrote_and_refuses_an_unknown_format_by_name`; `A/src/build_state.rs::tests::a_same_admin_restart_replays_the_journal_to_the_same_view` |

### Runtime / death / provider domain

| bullet | proving cell |
|---|---|
| every current birth carries a typed RuntimeFact bound to NodeId + IncarnationId + DeploymentId + provider + control domain + locator | `crates/rdm-core/src/runtime.rs::tests::a_fact_is_exact_or_refused_by_name`; `crates/rdm-transport/src/topology.rs::tests::a_birth_keeps_its_runtime_and_a_restart_brings_a_new_one` |
| the provider produces the locator and the node publishes exactly one normalized RuntimeFact; no competing launcher-owned RuntimeFact authority | `T/runtime/successor_adoption.rs::a_successor_manages_births_it_never_launched` |
| `data_dir` converges to non-launcher authorities without becoming RuntimeFact identity | `T/runtime/successor_adoption.rs::a_successor_manages_births_it_never_launched` |
| a late successor adopts and manages current runtime without completed Build receipts | `T/elections/late_lower_node_id_admin.rs::a_lower_node_id_admin_born_after_a_build_wins_and_manages_its_births` |
| the Day-0 externally started admin publishes its exact provider runtime | `A/src/deployment/pipeline.rs::tests::day_zero_adopts_this_process_with_one_receipt_per_step` |
| process identity resists PID reuse; container identity is the immutable container id | `A/src/deployment/process.rs::tests::a_successor_controls_the_exact_process_and_never_a_recycled_pid`; `A/tests/container_proof.rs::a_container_is_its_docker_domain_and_immutable_id_and_a_reused_name_never_impersonates_it` |
| no PID/container id is global across provider domains | `A/src/deployment/provider.rs::tests::a_published_runtime_is_adopted_only_within_its_provider_and_control_domain` |
| a same-incarnation locator or provider-domain change is refused | `crates/rdm-transport/src/topology.rs::tests::a_birth_keeps_its_runtime_and_a_restart_brings_a_new_one` |
| same-domain successor control and cross-domain refusal | `A/tests/container_proof.rs::a_successor_in_the_same_docker_domain_adopts_inspects_and_controls_a_container_it_never_launched`; `A/tests/container_proof.rs::an_admin_in_another_docker_domain_refuses_the_locator_by_name_and_touches_nothing` |
| provider terminal state of the adopted exact runtime reaches lifecycle and recovery | `T/runtime/container_kill.rs::a_killed_container_is_proven_terminal_by_inspection_and_the_build_recreates_it` |
| failed RPC, dial, silence or a provider-domain mismatch is never death proof | `A/tests/drift_convergence.rs::a_silent_node_whose_runtime_runs_opens_no_attempt_and_is_never_replaced`; `A/src/drift.rs::tests::a_silent_birth_counts_short_only_once_its_exact_runtime_is_inspected_exited` |

Multi-host eligibility is not claimed: the remote-pool provider is proven only in
`A/src/deployment/provider.rs`'s tests (`RemotePool`, same-pool adoption), not across real hosts.

### Leadership

| bullet | proving cell |
|---|---|
| only Ready candidates are eligible | `A/src/election.rs::tests::the_lowest_ready_node_id_wins_and_nothing_else_counts` |
| node-admin Ready includes the current Build, RuntimeFacts and provider-domain hydration | `A/src/admin.rs::tests::an_admin_can_take_over_only_when_every_held_birth_is_adoptable_here`; `A/src/admin.rs::tests::an_admin_without_a_current_build_can_never_take_over` |
| joining admins never self-Ready; only the Day-0 root establishes Ready without existing Ready authority | `A/src/admin.rs::tests::only_the_day0_root_applies_its_own_meshs_pending` |
| the complete NodeId is the only election key | `A/src/election.rs::tests::ordinal_and_mesh_name_never_decide_a_primary`; `A/src/admin.rs::tests::ready_since_and_ordinal_never_change_a_winner` |
| provider domain, locality and runtime metadata are not election keys | `A/src/election.rs::tests::provider_locality_and_runtime_metadata_never_decide_a_primary` |
| the node-admin cohort winner is the mesh primary; only mesh primaries hold the fabric election | `T/elections/node_type_election.rs::every_mn_node_type_elects_the_lowest_ready_node_id_through_the_matrix`; `A/src/election.rs::tests::losing_a_winner_moves_each_primary_to_the_next_lowest` |
| a living seat holder keeps its seat (a lower NodeId never displaces it); a seat moves only by yield, committed transfer or proven loss | `T/elections/node_primary_update.rs::a_planned_hand_over_makes_the_same_mesh_successor_both_primaries_on_its_direct_answer`; `A/src/admin.rs::tests::a_leaving_primary_stays_both_primaries_in_the_readers_until_its_hand_over_is_held`; `T/elections/node_primary_update.rs::a_lost_fabric_primary_is_succeeded_by_the_surviving_admin_of_its_own_mesh` |
| Day 0, three-mesh, replacement and late-admin takeover are green | `T/elections/fabric_primary.rs::three_mesh_primaries_keep_the_day0_fabric_primary_and_elect_the_lowest_remaining_when_its_mesh_is_lost`; `T/elections/late_lower_node_id_admin.rs::a_lower_node_id_admin_born_after_a_build_wins_and_manages_its_births`; `T/elections/node_primary_update.rs::a_planned_hand_over_makes_the_same_mesh_successor_both_primaries_on_its_direct_answer` |
| no stale amendment or parallel election canon | `crates/rdm-node/tests/leadership.rs::node_base_holds_no_comparator` |

### Repair / recovery

| bullet | proving cell |
|---|---|
| held-member repair heals a partition while each side retains neighbours | `T/gossip/backbone.rs::gossip_repairs_a_lost_push_and_an_isolated_admin_refuses_builds_and_heals_to_one_birth_per_path` |
| a failed rejoin is bounded and never marks Dead | `crates/rdm-transport/src/topology.rs::tests::a_failed_rejoin_is_retried_on_a_bounded_schedule` |
| an unheard mesh is mesh investigation: a node-admin is created into the live mesh only on a carrier-edge-lost verdict | `T/mesh/peer_mesh_investigation.rs::a_peer_mesh_without_its_admins_gets_a_node_admin_only_on_a_carrier_edge_lost_verdict_after_two_probes`; `T/mesh/peer_mesh_investigation.rs::two_peer_meshes_whose_admins_are_lost_together_are_two_investigations_each_creating_its_node_admin`; `T/mesh/admins_lost_mid_build.rs::a_mesh_whose_admins_are_lost_mid_build_gets_a_node_admin_on_the_carrier_edge_lost_verdict_and_the_build_completes` (under a Build in flight, which completes under its own id) |
| an answering node-admin stops the investigation; members that stay silent are held | `T/mesh/peer_mesh_investigation.rs::an_alive_but_unheard_admin_answers_the_probe_and_the_investigation_stops`; `T/mesh/peer_mesh_investigation.rs::an_admin_that_answers_probe_two_stops_the_investigation_over_probe_ones_carrier_edge_lost`; `T/mesh/peer_mesh_investigation.rs::a_peer_mesh_whose_members_all_stay_silent_gets_its_node_admin_created_on_the_investigation_verdict` |
| a fully crashed mesh gets its node-admin only on exact provider exit proof | `A/tests/build_drive.rs::the_create_of_a_fully_crashed_meshs_first_node_admin_runs_on_the_fabric_primary_and_nothing_else_of_the_held_mesh_does`; `T/elections/fabric_takeover.rs::a_lost_meshs_node_admin_is_re_created_over_its_inherited_dir_when_its_runtime_exited` |
| both arms set degraded on the mesh and the fabric until the new mesh-primary reports | `A/tests/drift_convergence.rs::fully_crashed_mesh::the_fabric_primary_authors_degraded_when_it_claims_a_fully_crashed_meshs_node_admin_and_only_its_new_primarys_report_clears_it`; `T/mesh/peer_mesh_investigation.rs::a_joined_node_admin_whose_planned_member_has_not_checked_in_keeps_degraded_until_it_does`; `A/src/checklist.rs::tests::degraded_clears_only_on_the_current_primarys_report_after_the_decision` |
| nobody publishes a mesh on its behalf; the new mesh-primary re-gossips once established | `crates/rdm-transport/tests/no_acting_publication.rs::the_fabric_primary_executing_a_peer_meshs_stop_and_delete_publishes_nothing_for_that_mesh`; `T/mesh/peer_mesh_investigation.rs::elected_primary_holds_its_checklist_for_the_killed_primary_then_republishes_the_old_status` |
| a node recovery proves the exact exit, then deletes and creates at the same path.name with a new node.id over the inherited data dir | `T/build/accepted_topology.rs::the_current_build_read_from_the_build_facts_is_the_one_accepted_topology` (step 7); `A/tests/node_rows.rs::a_recovered_nodes_row_is_deleted_and_the_new_node_at_its_path_name_claims_it_seeded` |
| exact-runtime work in another provider domain never guesses a local PID or container | `A/tests/container_proof.rs::an_admin_in_another_docker_domain_refuses_the_locator_by_name_and_touches_nothing` |
| fabric authority and the local executor are distinct | `T/mesh/mesh_create.rs::creating_a_mesh_hands_its_members_to_its_own_primary`; `A/src/executor.rs::tests::a_meshs_members_are_its_own_primarys_and_everything_else_the_fabric_primarys` |
| nothing inside the mesh checks who sent a frame or call; an out-of-order gossip frame is ignored once, in the receive path | `tools/mesh-audit/tests/outcome_codes.rs::permission_denied_and_unauthenticated_are_never_constructed`; `crates/rdm-transport/tests/publisher_handover.rs::a_frame_published_before_what_the_receiver_holds_of_its_source_is_ignored_by_the_receive_path` |
| the current Build remains readable independently of older Build history | `T/build/accepted_topology.rs::the_current_build_read_from_the_build_facts_is_the_one_accepted_topology` |

### Transport / Node RPC

| bullet | proving cell |
|---|---|
| one canonical Iroh Endpoint per process | `crates/rdm-transport/tests/one_endpoint.rs::one_endpoint_and_one_socket_serve_every_call_on_one_connection` |
| no second generic internal mTLS stack | `tools/mesh-audit/tests/dependency_rules.rs::rule6_a_member_depending_directly_on_a_tls_stack_fails_and_iroh_s_own_passes` |
| no hidden N0 DNS/Pkarr product address authority | `tools/mesh-audit/tests/address_lookup.rs::dns_and_pkarr_lookups_are_refused_anywhere`; `tools/mesh-audit/tests/address_lookup.rs::the_rdm_tree_configures_no_n0_lookup_and_keeps_canonical_crates_off_the_legacy_plane` |
| Direct/Proxy connections parity and target-preserving Node RPC composition | `crates/rdm-core/tests/connections.rs::the_route_prefers_a_valid_proxy_then_an_active_direct_then_none`; `crates/rdm-transport/tests/forward.rs::a_carried_call_reaches_the_target_once_and_the_target_sees_the_carrier`; `crates/rdm-transport/tests/context.rs::a_carried_call_keeps_the_origins_trace_and_caller_system_through_the_hop` |
| complete-send certainty, 499, pool supersession and carried execution | `crates/rdm-transport/tests/certainty.rs::reply_loss_after_a_complete_send_is_indeterminate`; `crates/rdm-transport/tests/certainty.rs::an_unfinished_send_resets_with_499_is_not_sent_and_is_never_dispatched`; `crates/rdm-transport/tests/pool.rs::late_superseded_dial_never_pools`; `crates/rdm-transport/tests/delivery.rs::a_carried_calls_resend_through_another_carrier_is_suppressed_by_its_origin_identity` |

The downstream-binding subsection of §14 is proven in i141's own import manifest (rafka-v2), not here.

## Ops and contracts the export carries

All are RDM-owned (`crates/rdm-core/src/catalog.rs:82-87`: `0x1B` status, `0x1C` build-claim, `0x1D` join,
`0x1E` topology, `0x1F` build-facts, `0x20` build, each `OpOwner::Product("rdm")`, `OpState::Live`).

| ruling | contract | proving cell |
|---|---|---|
| R-J1 `0x1D` JoinNode carries the digest; the entry ALPN is retired | `crates/rdm-core/src/join.rs:59` | `crates/rdm-core/tests/join_wire.rs::replies_match_the_frozen_eleven_variant_wire_schema`; `A/tests/join_admission.rs::join_refuses_a_digest_that_disagrees_with_the_deployment_and_installs_the_reported_address_on_a_match` |
| R-T6 `0x1E` GetTopology served by every node | `crates/rdm-core/src/topology.rs:80` | `crates/rdm-core/tests/topology_wire.rs::replies_match_the_frozen_fifteen_variant_wire_schema`; `A/tests/topology_read.rs::a_large_topology_streams_in_chunks_under_the_ceiling_and_installs_whole`; `T/rpc/topology_get.rs::born_node_joins_its_maker_then_reads_the_makers_topology_and_installs_it` |
| R-G6 `0x1F` FetchBuildFacts | `crates/rdm-core/src/build_facts.rs:56` | `crates/rdm-core/tests/build_facts_wire.rs::a_full_chunk_fits_the_reply_frame_ceiling`; `A/tests/build_facts_read.rs::a_build_the_entry_named_is_held_current_once_its_facts_are_fetched`; `T/build/joiner_build_facts.rs::joiners_whose_build_catch_up_is_lost_fetch_the_facts_and_the_build_completes` |
| R-W1 every internal frame is postcard | one codec for RPC and gossip | `A/tests/gossip_frames_wire.rs::every_gossip_frame_and_build_message_round_trips_through_postcard_under_the_ceiling`; the frozen-schema cells above |
| R-F2 Forward carries the remaining budget; the carrier answers within its reserve | `crates/rdm-transport/src/forward.rs:93` | `crates/rdm-transport/tests/forward.rs::the_forward_frame_carries_the_budget_that_remains_never_the_default`; `crates/rdm-transport/tests/forward_spans.rs::a_budget_spent_to_the_reply_reserve_is_refused_by_name_with_no_inner_call` |
| R-C1 the carrier names `CarrierEdgeLost` | `crates/rdm-transport/src/forward.rs` | `crates/rdm-transport/tests/forward.rs::a_carrier_whose_own_edge_to_the_target_is_not_active_refuses_carrier_edge_lost`; `…::a_carried_call_answering_carrier_edge_lost_hands_the_proxy_back_to_be_disconnected` |
| R-S2 / R-S3 event-driven declarations; a new primary adopts the existing status | `A/src/status_declare.rs` | `crates/rdm-transport/tests/ready_gate.rs::a_closed_ready_gate_refuses_ping_typed_and_an_open_gate_serves_it`; `A/tests/status_declarations.rs::a_node_declares_its_own_state_forward_only_and_applied_is_on_its_row_first` |
| R-G4 / R-G5 iroh-gossip pending-neighbor | fork commits `61a8cbb`, `4b6f558`, both in the pinned `508fb795` (through its ancestor `dce7b706`) | RED-then-GREEN cells in the fork (`2f75b32`, `b551ead`, both in `508fb795`) |

## Red, with owner

| item | why it is red | owner |
|---|---|---|
| the gates at the export SHA (fast, release, container proof, 30-minute seeded soak) | **closed 2026-10-20:** green at `abc1ec15` (the gate record above) | e10.s2, the export |
| `export_manifest_rejects_pending_or_unclassified_parity` and `export_manifest_certifies_one_candidate_with_all_owner_receipts`, job `i143-2790-export` | **closed 2026-10-20:** the job's receipt `ok` at `abc1ec15` with the other 76 owners'; `export-manifest --candidate abc1ec15… --repo /home/admin/rafka-v2` certified it (`eligible=true`, 0 refusals): the parity scan eligible, every owner's receipt `ok` there with no uncommitted path and its artifacts hashing, the fast and release gate logs at that SHA proving every cell of §14 above (81). The manifest it wrote is [i143-export-manifest.json](i143-export-manifest.json) | e10.s2 (#2790) |
| the Rafka-shaped qualification `i143-rshape-export-qualification` and its 20-node C1-C20 / S1-S5 matrix | **closed 2026-10-20:** run at `abc1ec15` as the export run's `rshape_jobs`, 34 receipts `ok`: the definition and consumer-import checks and the consumer build, the nine adversarial stories, then fast, composition, the four chaos stories and the five soak stories, each in both providers | #2943, #2949 |
| `docs/architecture/node-identity-and-state.md` legacy-section reconciliation and the other import-doc bullets of the issue | not audited in this pass | e10.s2 (#2790) |
| multi-host provider eligibility | not claimed (see the Runtime section above) | no owner; the claim is withheld |
