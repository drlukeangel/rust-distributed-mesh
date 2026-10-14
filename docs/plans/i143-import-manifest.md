# i143 import manifest

**Pinned export SHA:** `5532aabd535753ef37dace974af8b4de84203e3e` (RDM `main`), the revision rafka-v2 `Cargo.toml` pins. **Parity-through:** rafka-v2 `67755f3cdf41c1993da07c65761ca20dd02aa544`.
**Candidate for the next pin:** RDM `4014e36` (357 commits past the pinned SHA). It is not certified; see "Candidate 4014e36" below.
The eight imported packages, at the pinned revision in rafka-v2 `Cargo.toml` and `Cargo.lock`:
`rafka-node-rpc-contract`, `rafka-mesh-entity`, `rafka-mesh-transport`, `rafka-node-rpc`,
`rafka-node-admin-core`, `rafka-node-admin-client`, `rafka-test-scenario`, `rafka-node-rpc-testkit`.
Forks: `iroh` / `iroh-base` at `drlukeangel/iroh` `1d27e4ea25`, `iroh-gossip` at `drlukeangel/iroh-gossip` `4b6f5588d5` (RDM `Cargo.toml:52-54`); divergence and removal conditions in `docs/eng/vendor-divergence.md`.
## Gate record at the export SHA

| gate | result | evidence |
|---|---|---|
| parity scan (`parity-scan --through 195779e979bb207064d23fefe977f5f11e515dde`, run at export) | 193 boundary commits, 193 rows, 0 MIRROR pending, 0 violations, `eligible=true` | `docs/plans/i143-transport-parity.md` |
| fast gate (test cadence: staleness 3 s, gossip 500 ms) | 28/28 green | `gates-land-5532aab.log` |
| release gate (production windows: staleness 30 s, gossip 2 s) | 29/29 green | `gates-land-5532aabrelease.log` |
| 30-minute seeded soak, all eight process faults incl. fabric-primary loss and admin wedge (`RAFKA_SOAK_SEED=7 RAFKA_SOAK_SECS=1800`) | every invariant held: 32 rounds, 10 runtime losses, 3 attempts of the accepted Build, nothing left running | `fabric-soak/seeded` artifacts |
| container proof (PRD §12.2 cells, Docker domain) | ran in the fast gate's `container-proof` job: 3 provider cells + the real `docker kill` cell, green | `crates/rafka-node-admin-core/tests/container_proof.rs`, `mesh_runtime__container_kill` |

The soak ran at `6c77b567d7246d2d5e92e100b0c14a60c846f5b8`. The export SHA adds only:
- five cells;
- two pure functions lifted out unchanged (`admin.rs::pending_gate`, `membership.rs::refeed_backoff`);
- the soak's own evidence selector, which now reads the target's 425 span (`receiver_node_id`) and no longer the caller's same-family refusal.

No runtime path changed between the two.

## §14 export gate: every bullet and the cell that proves it

Paths are RDM: `S/` = `crates/rafka-test-scenario/tests/`, `AC/` = `crates/rafka-node-admin-core/`.

### Identity

| bullet | proving cell |
|---|---|
| NodeId/MeshId/FabricId are canonical Crockford60 typed ids | `S/mesh_identity__canonical_ids.rs::every_product_identity_is_canonical_and_kept_or_reminted_by_its_lifecycle`; `crates/rafka-mesh-entity/tests/crockford60_parity.rs::every_rafka_minted_id_is_a_node_mesh_and_fabric_id_as_is` |
| EndpointId is a distinct transport identity | `S/mesh_identity__canonical_ids.rs::every_product_identity_is_canonical_and_kept_or_reminted_by_its_lifecycle` |
| no canonical 32-char hex logical product id | `crates/rafka-mesh-entity/src/ids.rs::tests::deserializing_refuses_a_non_canonical_product_id` |
| recovery keeps MeshId/FabricId; restart keeps NodeId; replacement re-mints | `S/mesh_lifecycle__mesh_recover.rs::a_lost_mesh_recovers_as_itself_under_the_same_build`; `S/mesh_identity__canonical_ids.rs` (above) |

### Accepted topology / Build

| bullet | proving cell |
|---|---|
| the fabric-primary is the only topology writer; others refused by name | `AC/tests/control_routes.rs::a_node_admin_that_is_not_the_fabric_primary_refuses_every_topology_change_by_name` |
| `Fabric.build_id` names one complete Build; no other desired-topology authority | `S/fabric_build__accepted_topology.rs::the_build_fabric_build_id_names_is_the_one_accepted_topology` |
| complete Build persistence precedes the `Fabric.build_id` write | `AC/tests/control_routes.rs::a_build_the_store_refuses_is_never_named_by_fabric_build_id` |
| every admin hydrates the Fabric record + Build before authority-bearing Ready | `S/mesh_elections__late_authority.rs::a_lower_node_id_admin_born_after_a_build_wins_and_manages_its_births`; `AC/src/admin.rs::tests::an_admin_without_a_hydrated_fabric_pointer_is_never_authority_capable` |
| older Build history is never replayed | `S/fabric_build__accepted_topology.rs::the_build_fabric_build_id_names_is_the_one_accepted_topology` |
| convenience APIs compile into a new complete Build | `AC/tests/control_routes.rs::a_change_compiles_to_the_next_build_one_at_a_time_and_a_restart_opens_an_attempt`; `…::post_build_and_mesh_routes_submit_build` |
| a second change while reconciling is `409 build-in-progress` | `AC/tests/control_routes.rs::a_change_compiles_to_the_next_build_one_at_a_time_and_a_restart_opens_an_attempt` |
| authority movement, restart, replacement and drift use another attempt of the same Build | `S/fabric_build__accepted_topology.rs` (above); `S/mesh_lifecycle__mesh_recover.rs::a_lost_mesh_recovers_as_itself_under_the_same_build` |
| `DELETE /api/builds?id=B` refused while `Fabric.build_id == B` | `AC/tests/control_routes.rs::build_history_is_readable_and_only_finished_builds_that_are_not_current_can_be_forgotten` |
| `GET /api/fabric` exposes `build_id` | `AC/tests/control_routes.rs::a_change_compiles_to_the_next_build_one_at_a_time_and_a_restart_opens_an_attempt` |
| all-admin restart restores Fabric record, accepted Build and FabricShutdown | `AC/src/fabric_storage.rs::tests::the_file_storage_reloads_what_it_wrote_and_refuses_an_unknown_format_by_name`; `AC/src/build_state.rs::tests::a_same_admin_restart_replays_the_journal_to_the_same_view` |

### Runtime / death / provider domain

| bullet | proving cell |
|---|---|
| a typed RuntimeFact bound to NodeId + IncarnationId + DeploymentId + provider + domain + locator | `crates/rafka-mesh-entity/src/runtime.rs::tests::a_fact_is_exact_or_refused_by_name`; `crates/rafka-mesh-transport/src/membership.rs::tests::a_birth_keeps_its_runtime_and_a_restart_brings_a_new_one` |
| provider produces the locator; the node publishes the one normalized fact; no launcher-owned authority | `S/mesh_runtime__successor_adoption.rs::a_successor_manages_births_it_never_launched` |
| `data_dir` reaches non-launcher authorities | `S/mesh_runtime__successor_adoption.rs::a_successor_manages_births_it_never_launched` |
| a late successor adopts without Build receipts | `S/mesh_elections__late_authority.rs::a_lower_node_id_admin_born_after_a_build_wins_and_manages_its_births` |
| the Day-0 admin adopts and publishes its own runtime | `AC/src/deployment/pipeline.rs::tests::day_zero_adopts_this_process_with_one_receipt_per_step` |
| PID-reuse resistance; immutable container id | `AC/src/deployment/process.rs::tests::a_successor_controls_the_exact_process_and_never_a_recycled_pid`; `AC/tests/container_proof.rs::a_container_is_its_docker_domain_and_immutable_id_and_a_reused_name_never_impersonates_it` |
| no PID/container id is global across domains | `AC/src/deployment/provider.rs::tests::a_published_runtime_is_adopted_only_within_its_provider_and_control_domain` |
| a same-incarnation locator or domain change is refused | `crates/rafka-mesh-transport/src/membership.rs::tests::a_birth_keeps_its_runtime_and_a_restart_brings_a_new_one` |
| same-domain successor control, cross-domain refusal | `AC/tests/container_proof.rs::a_successor_in_the_same_docker_domain_adopts_inspects_and_controls_a_container_it_never_launched`; `…::an_authority_in_another_docker_domain_refuses_the_locator_by_name_and_touches_nothing` |
| provider terminal state reaches recovery | `S/mesh_runtime__container_kill.rs::a_killed_container_is_proven_terminal_by_inspection_and_the_build_recreates_it` |
| failed RPC, silence or domain mismatch is never death proof | `AC/tests/drift_convergence.rs::a_silent_node_whose_runtime_runs_opens_no_attempt_and_is_never_replaced`; `AC/src/drift.rs::tests::a_silent_birth_is_held_until_its_exact_runtime_is_proven_exited` |

Multi-host eligibility is not claimed: the remote-pool provider is proven only in
`AC/src/deployment/provider.rs` (same-pool adoption), not across real hosts.

### Leadership

| bullet | proving cell |
|---|---|
| only ReadyForTraffic candidates | `AC/src/election.rs::tests::the_lowest_ready_node_id_wins_and_nothing_else_counts` |
| admin Ready needs `Fabric.build_id`, its Build, RuntimeFacts and provider-domain capability | `AC/src/admin.rs::tests::an_admin_is_authority_capable_only_when_every_held_birth_is_adoptable_here`; `…::an_admin_without_a_hydrated_fabric_pointer_is_never_authority_capable` |
| joining admins never self-Ready; only the Day-0 root self-applies its mesh's Pending | `AC/src/admin.rs::tests::only_the_day0_root_applies_its_own_meshs_pending` |
| the complete NodeId is the only election key | `AC/src/election.rs::tests::ordinal_and_mesh_name_never_decide_a_seat`; `AC/src/admin.rs::tests::ready_since_and_ordinal_never_change_a_winner` |
| provider domain, locality and runtime metadata are not election keys | `AC/src/election.rs::tests::provider_locality_and_runtime_metadata_never_decide_a_seat` |
| cohort winner == mesh primary; only mesh primaries hold the fabric election | `S/mesh_elections__cohort_election.rs::every_mn_cohort_elects_the_lowest_ready_node_id_through_the_matrix`; `AC/src/election.rs::tests::losing_a_winner_moves_each_seat_to_the_next_lowest` |
| Day 0, three-mesh, preemption/replacement, late-admin takeover | `S/mesh_elections__fabric_primary.rs::three_mesh_primaries_elect_the_lowest_node_id_and_fail_over_to_the_next`; `S/mesh_elections__late_authority.rs` (above) |
| s14 non-launcher adoption (#2850/#2851) is green | `S/mesh_runtime__successor_adoption.rs::a_successor_manages_births_it_never_launched` |
| no parallel election canon | `crates/rafka-node-base/tests/leadership.rs::node_base_holds_no_comparator` |

### Repair / recovery

| bullet | proving cell |
|---|---|
| held-member repair heals a partition while each side keeps neighbours | `S/mesh_membership__backbone.rs::gossip_repairs_a_lost_push_and_isolation_authorizes_nothing` |
| a failed rejoin is bounded and never marks Dead | `crates/rafka-mesh-transport/src/membership.rs::tests::a_failed_rejoin_is_retried_on_a_bounded_schedule`; `AC/src/offline.rs::tests::true_offline_needs_two_rounds_with_no_path_at_least_one_floor_apart` |
| fabric election completes before recovery | `AC/tests/drift_convergence.rs::a_lost_fabric_primary_is_proven_and_reborn_by_its_successor` |
| recovery: identity → hydration → Pending Applied → reconcile → Ready → election | `S/mesh_lifecycle__mesh_recover.rs::a_lost_mesh_recovers_as_itself_under_the_same_build`; `S/mesh_lifecycle__mesh_create.rs::creating_a_mesh_hands_its_members_to_its_own_primary` |
| a Pending executor claims no other mesh's or fabric-wide work | `AC/src/executor.rs::tests::a_meshs_members_are_its_own_primarys_and_everything_else_the_fabric_primarys`; `AC/tests/status_declarations.rs::mesh_and_fabric_declarations_follow_the_same_rules` |
| cross-domain work never guesses a local PID/container | `AC/tests/container_proof.rs::an_authority_in_another_docker_domain_refuses_the_locator_by_name_and_touches_nothing` |
| fabric authority and local recovery executor are distinct | `S/mesh_lifecycle__mesh_create.rs::creating_a_mesh_hands_its_members_to_its_own_primary` |
| the accepted Build stays recoverable without older history | `S/fabric_build__accepted_topology.rs::the_build_fabric_build_id_names_is_the_one_accepted_topology` |

## Candidate 4014e36

RDM `main` `4014e36`; rafka-v2 parity-through `67755f3cdf`.

| check | result |
|---|---|
| `parity-scan --through 67755f3cdf` (rows for `652d6595f8` pin, `9706b10b38` broker entry, `42b094b541` ResyncChange deleted, `1baa9efe74` + `22da068ff2` a change and its revert, `0ebf230119` `32dea4ced5` `53dcb6b75c` `5e5bdbd8c4` fabric state-sync lifecycle, `4968fc4b5e` `21baf663c1` writer lease; all RAFKA DOMAIN in `docs/plans/i143-transport-parity.md`) | 204 boundary commits, 204 rows, 0 MIRROR pending, 0 violations, `eligible=true` |
| every cell this manifest names exists at `4014e36` | 47 of 47 cell function names found by `fn <name>` under `crates/` and `tools/`; every `S/` and `AC/` file path exists |
| `cargo test -p rafka-mesh-audit` at `4014e36` | all test binaries green, 96 tests (parity_scanner, lock_ratchets, connections_parity, dependency_rules, acceptance_gate, rshape_definition, address_lookup, legacy_binaries, admin_ui_client) |

### Ops and contracts landed since the pinned SHA

All are RDM-owned (`crates/rafka-node-rpc-contract/src/catalog.rs:76-80`: `0x1B` status, `0x1C` build-claim, `0x1D` join, `0x1E` topology, `0x1F` build-facts, each `OpOwner::Product("rdm")`, `OpState::Live`).

| ruling | contract | proving cell |
|---|---|---|
| R-J1 `0x1D` JoinNode carries the digest; the entry ALPN is retired | `crates/rafka-node-rpc-contract/src/join.rs:78` | `crates/rafka-node-rpc-contract/tests/join_wire.rs::replies_match_the_frozen_nine_variant_wire_schema`; `crates/rafka-node-admin-core/tests/i143_rj1_join.rs::join_refuses_a_digest_that_disagrees_with_the_deployment_and_installs_the_reported_address_on_a_match` |
| R-T6 `0x1E` GetTopology served by every node | `.../src/topology.rs:118` | `.../tests/topology_wire.rs::replies_match_the_frozen_twelve_variant_wire_schema`; `crates/rafka-node-admin-core/tests/topology_read.rs::a_large_topology_streams_in_chunks_under_the_ceiling_and_installs_whole`; `crates/rafka-test-scenario/tests/i143_acceptance_rt6.rs::born_node_joins_its_maker_then_reads_the_makers_topology_and_installs_it` |
| R-G6 `0x1F` FetchBuildFacts | `.../src/build_facts.rs:86` | `.../tests/build_facts_wire.rs::a_full_chunk_fits_the_reply_frame_ceiling`; `crates/rafka-node-admin-core/tests/build_facts_read.rs::a_parked_pointer_is_resolved_by_fetching_the_builds_facts`; `crates/rafka-test-scenario/tests/i143_acceptance_rg6.rs::joiners_whose_build_catch_up_is_lost_fetch_the_facts_and_the_build_completes` |
| R-W1 every internal frame is postcard | one codec for RPC and gossip | `crates/rafka-node-admin-core/tests/i143_acceptance_rw1.rs::every_gossip_frame_and_build_message_round_trips_through_postcard_under_the_ceiling`; the `*_wire.rs` frozen-schema cells above |
| R-F2 Forward carries the remaining budget; the carrier answers within its reserve | `crates/rafka-node-rpc/src/forward.rs:110` | `crates/rafka-node-rpc/tests/forward.rs::the_forward_frame_carries_the_budget_that_remains_never_the_default`; `::a_budget_spent_to_the_reply_reserve_is_refused_by_name_with_no_inner_call` |
| R-C1 the carrier names `CarrierEdgeLost` | `.../forward.rs` | `crates/rafka-node-rpc/tests/forward.rs::a_carrier_whose_own_edge_to_the_target_is_not_active_refuses_carrier_edge_lost`; `::a_carried_call_answering_carrier_edge_lost_hands_the_proxy_back_for_retirement` |
| R-S2 / R-S3 event-driven declarations; a new primary adopts the existing status | `crates/rafka-node-admin-core/src/status_declare.rs` | `crates/rafka-node-rpc/tests/ready_gate.rs::a_closed_ready_gate_refuses_ping_typed_and_an_open_gate_serves_it`; `crates/rafka-node-admin-core/tests/status_declarations.rs::a_node_declares_its_own_state_forward_only_and_applied_is_on_its_row_first` |
| R-G4 / R-G5 iroh-gossip pending-neighbor | fork `61a8cbb`, `4b6f558` | RED-then-GREEN cells in the fork (`2f75b32`, `b551ead`); `docs/eng/vendor-divergence.md` |

The cells in the table exist at `4014e36`; they were not re-run for this manifest.

### Red, with owner

| item | why it is red | owner |
|---|---|---|
| the gates at `4014e36` (fast, release, container proof, 30-minute seeded soak) | not run at this SHA; the record above is for `5532aab` | the pin story that moves the pin, after the R-A2 ruling on the fabric-primary / claim area |
| `export_manifest_rejects_pending_or_unclassified_parity` and `export_manifest_certifies_one_candidate_with_all_owner_receipts` in `tools/mesh-audit/tests/i143_acceptance_2790.rs`, job `i143-2790-export` | the file and the registry row do not exist in RDM (`tools/mesh-audit/tests`, `tools/mesh-audit/i143-acceptance-jobs.json`) | #2790, after #2949 |
| the Rafka-shaped qualification `i143-rshape-export-qualification` and its 20-node C1-C20 / S1-S5 matrix | #2943 and #2949 are open | #2943, #2949 |
| `docs/architecture/node-identity-and-state.md` legacy-section reconciliation and the other import-doc bullets of the issue | not audited in this pass | #2790 |
| multi-host provider eligibility | not claimed (see Runtime section above) | no owner; the claim is withheld |

Fabric-primary, election and claim code (`election.rs`, `build_claim.rs`, the `0x1C` family) changed in the 357 commits (R-S3, R-G6); the manifest names its cells and does not alter it. Any change there waits for R-A2.
