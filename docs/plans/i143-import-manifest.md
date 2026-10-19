# i143 import manifest

**Export SHA:** at export (RDM `main`), the revision rafka-v2 `Cargo.toml` pins. **Parity-through:** rafka-v2 at export.
The six imported packages, at the export revision in rafka-v2 `Cargo.toml` and `Cargo.lock`:
`rdm-core`, `rdm-transport`, `rdm-node`, `rdm-admin`, `rdm-client`, `rdm-test`.
Forks: `iroh` / `iroh-base` at `drlukeangel/iroh` `23ab5a84`, `iroh-gossip` at `drlukeangel/iroh-gossip` `dce7b706` (RDM `Cargo.toml:51-53`); each fork's divergence and its removal condition are stated beside its pin (`Cargo.toml:33-50`).

## Gate record at the export SHA

| gate | result | evidence |
|---|---|---|
| parity scan (`parity-scan --through <parity-through>`) | at export | at export |
| fast gate (test cadence: staleness 3 s, gossip 500 ms) | at export | at export |
| release gate (the release-only jobs, each stem at its own cadence) | at export | at export |
| 30-minute seeded soak, all eight process faults incl. fabric-primary loss and admin wedge | at export | at export |
| container proof (PRD §12.2 cells, Docker domain: `crates/rdm-admin/tests/container_proof.rs`, `T/runtime/container_kill.rs`) | at export | at export |

## §14 export gate: every bullet and the cell that proves it

Every cell below exists at RDM `main` `5fa6623c` (2026-10-19). Paths are RDM's: `T/` = `crates/rdm-test/tests/`,
`A/` = `crates/rdm-admin/`. The REST control routes are retired: a bullet that names one is proven on the Node RPC
op that carries it, named beside the cell.

### Identity

| bullet | proving cell |
|---|---|
| NodeId/MeshId/FabricId are canonical Crockford60 typed ids | `T/mesh/canonical_ids.rs::every_product_identity_is_canonical_and_kept_or_reminted_by_its_lifecycle`; `crates/rdm-core/tests/crockford60_parity.rs::every_rafka_minted_id_is_a_node_mesh_and_fabric_id_as_is` |
| EndpointId is a distinct transport identity | `T/mesh/canonical_ids.rs::every_product_identity_is_canonical_and_kept_or_reminted_by_its_lifecycle` |
| no canonical 32-char hex logical product id | `crates/rdm-core/src/ids.rs::tests::deserializing_refuses_a_non_canonical_product_id` |
| recovery keeps MeshId/FabricId; restart keeps NodeId; replacement re-mints | `T/mesh/mesh_recover.rs::a_lost_mesh_recovers_as_itself_under_the_same_build`; `T/mesh/canonical_ids.rs` (above) |

### Accepted topology / Build

| bullet | proving cell |
|---|---|
| the fabric-primary is the only topology writer; others refused by name | `A/tests/node_objects.rs::a_build_call_to_an_admin_without_the_fabric_seat_is_refused_under_its_own_reject_span_naming_the_fabric_primary` (`not-fabric-primary`) |
| `Fabric.build_id` names one complete Build; no other desired-topology authority | `T/build/accepted_topology.rs::the_build_fabric_build_id_names_is_the_one_accepted_topology` (`fabric.get`'s `build_id`) |
| an accepted Build is current from the Build store alone, durable before it is answered (R-BI3; the `Fabric.build_id` pointer is retired) | `A/tests/current_build.rs::an_accepted_build_is_current_from_the_build_store_alone` (R-BI3: the Fabric row carries no pointer; a Build is written once and is current from the builds store) |
| every admin hydrates the Fabric record + Build before authority-bearing Ready | `T/elections/late_authority.rs::a_lower_node_id_admin_born_after_a_build_wins_and_manages_its_births`; `A/src/admin.rs::tests::an_admin_without_a_current_build_is_never_authority_capable` |
| older Build history is never replayed | `T/build/accepted_topology.rs::the_build_fabric_build_id_names_is_the_one_accepted_topology` |
| convenience APIs compile into a new complete Build | `A/src/accepted.rs::tests::a_submitted_change_compiles_to_exact_paths_and_a_shrink_keeps_the_primary`; `…::mesh_changes_compile_and_the_refusals_are_named`; `A/tests/current_build.rs::a_change_after_a_failed_build_compiles_against_the_newest_completed_build` |
| a second change while reconciling is refused `BuildInProgress` naming the current Build (Node RPC; the REST 409 is retired) | `A/tests/build_admission.rs::two_concurrent_topology_submissions_admit_one_build_and_refuse_the_other_naming_it` (`BuildInProgress` naming the first) |
| authority movement, restart, replacement and drift use another attempt of the same Build | `T/build/accepted_topology.rs` (above); `T/mesh/mesh_recover.rs::a_lost_mesh_recovers_as_itself_under_the_same_build` |
| `build.delete` of the current accepted Build is refused `cannot-delete` (Node RPC; the REST route and the `Fabric.build_id` pointer are retired, R-BI3) | `T/build/accepted_topology.rs::the_build_fabric_build_id_names_is_the_one_accepted_topology` (`build.delete` of the accepted Build answers `cannot-delete`) |
| `fabric.get` names the current Build (Node RPC; the REST route is retired) | `T/build/accepted_topology.rs::the_build_fabric_build_id_names_is_the_one_accepted_topology` (`fabric.get`'s `build_id`) |
| all-admin restart restores Fabric record, accepted Build and FabricShutdown | `A/src/fabric_storage.rs::tests::the_file_storage_reloads_what_it_wrote_and_refuses_an_unknown_format_by_name`; `A/src/build_state.rs::tests::a_same_admin_restart_replays_the_journal_to_the_same_view` |

### Runtime / death / provider domain

| bullet | proving cell |
|---|---|
| a typed RuntimeFact bound to NodeId + IncarnationId + DeploymentId + provider + domain + locator | `crates/rdm-core/src/runtime.rs::tests::a_fact_is_exact_or_refused_by_name`; `crates/rdm-transport/src/membership.rs::tests::a_birth_keeps_its_runtime_and_a_restart_brings_a_new_one` |
| provider produces the locator; the node publishes the one normalized fact; no launcher-owned authority | `T/runtime/successor_adoption.rs::a_successor_manages_births_it_never_launched` |
| `data_dir` reaches non-launcher authorities | `T/runtime/successor_adoption.rs::a_successor_manages_births_it_never_launched` |
| a late successor adopts without Build receipts | `T/elections/late_authority.rs::a_lower_node_id_admin_born_after_a_build_wins_and_manages_its_births` |
| the Day-0 admin adopts and publishes its own runtime | `A/src/deployment/pipeline.rs::tests::day_zero_adopts_this_process_with_one_receipt_per_step` |
| PID-reuse resistance; immutable container id | `A/src/deployment/process.rs::tests::a_successor_controls_the_exact_process_and_never_a_recycled_pid`; `A/tests/container_proof.rs::a_container_is_its_docker_domain_and_immutable_id_and_a_reused_name_never_impersonates_it` |
| no PID/container id is global across domains | `A/src/deployment/provider.rs::tests::a_published_runtime_is_adopted_only_within_its_provider_and_control_domain` |
| a same-incarnation locator or domain change is refused | `crates/rdm-transport/src/membership.rs::tests::a_birth_keeps_its_runtime_and_a_restart_brings_a_new_one` |
| same-domain successor control, cross-domain refusal | `A/tests/container_proof.rs::a_successor_in_the_same_docker_domain_adopts_inspects_and_controls_a_container_it_never_launched`; `…::an_authority_in_another_docker_domain_refuses_the_locator_by_name_and_touches_nothing` |
| provider terminal state reaches recovery | `T/runtime/container_kill.rs::a_killed_container_is_proven_terminal_by_inspection_and_the_build_recreates_it` |
| failed RPC, silence or domain mismatch is never death proof | `A/tests/drift_convergence.rs::a_silent_node_whose_runtime_runs_opens_no_attempt_and_is_never_replaced`; `A/src/drift.rs::tests::a_silent_birth_is_held_until_its_exact_runtime_is_proven_exited` |

Multi-host eligibility is not claimed: the remote-pool provider is proven only in
`A/src/deployment/provider.rs`'s tests (`RemotePool`, same-pool adoption), not across real hosts.

### Leadership

| bullet | proving cell |
|---|---|
| only ReadyForTraffic candidates | `A/src/election.rs::tests::the_lowest_ready_node_id_wins_and_nothing_else_counts` |
| admin Ready needs `Fabric.build_id`, its Build, RuntimeFacts and provider-domain capability | `A/src/admin.rs::tests::an_admin_is_authority_capable_only_when_every_held_birth_is_adoptable_here`; `…::an_admin_without_a_current_build_is_never_authority_capable` |
| joining admins never self-Ready; only the Day-0 root self-applies its mesh's Pending | `A/src/admin.rs::tests::only_the_day0_root_applies_its_own_meshs_pending` |
| the complete NodeId is the only election key | `A/src/election.rs::tests::ordinal_and_mesh_name_never_decide_a_seat`; `A/src/admin.rs::tests::ready_since_and_ordinal_never_change_a_winner` |
| provider domain, locality and runtime metadata are not election keys | `A/src/election.rs::tests::provider_locality_and_runtime_metadata_never_decide_a_seat` |
| cohort winner == mesh primary; only mesh primaries hold the fabric election | `T/elections/cohort_election.rs::every_mn_cohort_elects_the_lowest_ready_node_id_through_the_matrix`; `A/src/election.rs::tests::losing_a_winner_moves_each_seat_to_the_next_lowest` |
| Day 0, three-mesh, replacement, late-admin takeover; a lower NodeId never preempts a living holder (R-A2) | `T/elections/fabric_primary.rs::three_mesh_primaries_keep_the_day0_holder_and_elect_the_lowest_remaining_when_its_mesh_is_lost` (R-A2: a lower NodeId never displaces a living holder, so no preemption); `T/elections/late_authority.rs` (above) |
| s14 non-launcher adoption (#2850/#2851) is green | `T/runtime/successor_adoption.rs::a_successor_manages_births_it_never_launched` |
| no parallel election canon | `crates/rdm-node/tests/leadership.rs::node_base_holds_no_comparator` |

### Repair / recovery

| bullet | proving cell |
|---|---|
| held-member repair heals a partition while each side keeps neighbours | `T/gossip/backbone.rs::gossip_repairs_a_lost_push_and_isolation_authorizes_nothing` |
| a failed rejoin is bounded and never marks Dead | `crates/rdm-transport/src/membership.rs::tests::a_failed_rejoin_is_retried_on_a_bounded_schedule`; `A/src/offline.rs::tests::true_offline_needs_two_rounds_with_no_path_at_least_one_floor_apart` |
| the fabric seat's takeover commits before the lost mesh is re-created | `T/elections/fabric_takeover.rs::a_lost_meshs_node_admin_is_re_created_from_the_fabric_lane_over_a_new_dir_with_inherited_false` (the re-creation is admitted only on the committed takeover); `A/tests/drift_convergence.rs::a_lost_fabric_primary_is_recovered_by_its_own_meshs_new_primary` (a fabric-primary that handed its seats off before it exited) |
| recovery: identity → hydration → Pending Applied → reconcile → Ready → election | `T/mesh/mesh_recover.rs::a_lost_mesh_recovers_as_itself_under_the_same_build`; `T/mesh/mesh_create.rs::creating_a_mesh_hands_its_members_to_its_own_primary` |
| a Pending executor claims no other mesh's or fabric-wide work | `A/src/executor.rs::tests::a_meshs_members_are_its_own_primarys_and_everything_else_the_fabric_primarys`; `A/tests/status_declarations.rs::mesh_and_fabric_declarations_follow_the_same_rules` |
| cross-domain work never guesses a local PID/container | `A/tests/container_proof.rs::an_authority_in_another_docker_domain_refuses_the_locator_by_name_and_touches_nothing` |
| fabric authority and local recovery executor are distinct | `T/mesh/mesh_create.rs::creating_a_mesh_hands_its_members_to_its_own_primary` |
| the accepted Build stays recoverable without older history | `T/build/accepted_topology.rs::the_build_fabric_build_id_names_is_the_one_accepted_topology` |

## Ops and contracts the export carries

All are RDM-owned (`crates/rdm-core/src/catalog.rs:82-87`: `0x1B` status, `0x1C` build-claim, `0x1D` join,
`0x1E` topology, `0x1F` build-facts, `0x20` build, each `OpOwner::Product("rdm")`, `OpState::Live`).

| ruling | contract | proving cell |
|---|---|---|
| R-J1 `0x1D` JoinNode carries the digest; the entry ALPN is retired | `crates/rdm-core/src/join.rs:59` | `crates/rdm-core/tests/join_wire.rs::replies_match_the_frozen_eleven_variant_wire_schema`; `A/tests/join_admission.rs::join_refuses_a_digest_that_disagrees_with_the_deployment_and_installs_the_reported_address_on_a_match` |
| R-T6 `0x1E` GetTopology served by every node | `crates/rdm-core/src/topology.rs:80` | `crates/rdm-core/tests/topology_wire.rs::replies_match_the_frozen_fourteen_variant_wire_schema`; `A/tests/topology_read.rs::a_large_topology_streams_in_chunks_under_the_ceiling_and_installs_whole`; `T/rpc/topology_get.rs::born_node_joins_its_maker_then_reads_the_makers_topology_and_installs_it` |
| R-G6 `0x1F` FetchBuildFacts | `crates/rdm-core/src/build_facts.rs:56` | `crates/rdm-core/tests/build_facts_wire.rs::a_full_chunk_fits_the_reply_frame_ceiling`; `A/tests/build_facts_read.rs::a_build_the_entry_named_is_held_current_once_its_facts_are_fetched`; `T/build/joiner_build_facts.rs::joiners_whose_build_catch_up_is_lost_fetch_the_facts_and_the_build_completes` |
| R-W1 every internal frame is postcard | one codec for RPC and gossip | `A/tests/gossip_frames_wire.rs::every_gossip_frame_and_build_message_round_trips_through_postcard_under_the_ceiling`; the frozen-schema cells above |
| R-F2 Forward carries the remaining budget; the carrier answers within its reserve | `crates/rdm-transport/src/forward.rs:93` | `crates/rdm-transport/tests/forward.rs::the_forward_frame_carries_the_budget_that_remains_never_the_default`; `crates/rdm-transport/tests/forward_spans.rs::a_budget_spent_to_the_reply_reserve_is_refused_by_name_with_no_inner_call` |
| R-C1 the carrier names `CarrierEdgeLost` | `crates/rdm-transport/src/forward.rs` | `crates/rdm-transport/tests/forward.rs::a_carrier_whose_own_edge_to_the_target_is_not_active_refuses_carrier_edge_lost`; `…::a_carried_call_answering_carrier_edge_lost_hands_the_proxy_back_for_retirement` |
| R-S2 / R-S3 event-driven declarations; a new primary adopts the existing status | `A/src/status_declare.rs` | `crates/rdm-transport/tests/ready_gate.rs::a_closed_ready_gate_refuses_ping_typed_and_an_open_gate_serves_it`; `A/tests/status_declarations.rs::a_node_declares_its_own_state_forward_only_and_applied_is_on_its_row_first` |
| R-G4 / R-G5 iroh-gossip pending-neighbor | fork commits `61a8cbb`, `4b6f558`, both in the pinned `dce7b706` | RED-then-GREEN cells in the fork (`2f75b32`, `b551ead`, both in `dce7b706`) |

## Red, with owner

| item | why it is red | owner |
|---|---|---|
| the gates at the export SHA (fast, release, container proof, 30-minute seeded soak) | not run until the export SHA is chosen | e10.s2, the export |
| `export_manifest_rejects_pending_or_unclassified_parity` and `export_manifest_certifies_one_candidate_with_all_owner_receipts`, job `i143-2790-export` | neither the file `tools/mesh-audit/tests/i143_acceptance_2790.rs` nor its registry row in `tools/mesh-audit/i143-acceptance-jobs.json` exists at `5fa6623c` | e10.s2 (#2790) |
| the Rafka-shaped qualification `i143-rshape-export-qualification` and its 20-node C1-C20 / S1-S5 matrix | not run at an export SHA | #2943, #2949 |
| `docs/architecture/node-identity-and-state.md` legacy-section reconciliation and the other import-doc bullets of the issue | not audited in this pass | e10.s2 (#2790) |
| multi-host provider eligibility | not claimed (see the Runtime section above) | no owner; the claim is withheld |
