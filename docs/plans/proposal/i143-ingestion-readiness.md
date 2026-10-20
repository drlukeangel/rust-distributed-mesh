# i143 → rafka-v2 ingestion readiness: what the demo demonstrates, what it does not

**Asked:** Luke, 2026-10-20: "verify that the demo demonstrates that it can achieve everything we need to do
in 142 and 143 so it's ready for ingestion." **Author:** the lead, 2026-10-20, at `1feb0925` (release green:
109 ok, 0 FAIL; `gates-e4s8-1feb0925release.log`). **Method:** every row of rafka-v2's i141 package/adapter
table and e-blocks (`/home/admin/rafka-v2/docs/plans/i141-node-rpc-substrate-prd.md`), its §18 canaries, and
i142's moved-from-i143 rows, against the demo's cells (`demo/`, the pinned consumer workspace built by
`scripts/i143-rshape-build-consumer.sh`: no private RDM path, `rshape_consumer_imports_exact_candidate_without_private_paths`)
and the R-shape jobs that run its four executables (`crates/rdm-test/tests/rshape_burn_in.rs`,
`tools/mesh-audit/i143-acceptance-jobs.json` `rshape_jobs`). A row is **demonstrated** only when the demo
or an R-shape job exercises it through the public API at `1feb0925`; **RDM-proven** when only RDM's own
cells prove it (the import manifest's §14 table); **gap** when neither does.

## 1. What the demo is

Four role executables composed from RDM's public seams and nothing else (`demo/src/lib.rs`): `rshape-node-admin`
is `rdm_admin::entry::run` (all configuration the environment; cert material required or refused by name);
`rshape-gateway` / `-compute` / `-broker` are `rdm_node::launch_for` + `node::start_with_seams`, serving the
testkit's opaque proof families (0x70-0x73) with the carrier policy `Forwardable { carrier_kind: Gateway }`. The
estate (`Estate::bootstrap_external`) provisions nothing but the Day-0 admin with `RDM_CA_SECRET`; every other
node is born by a Build the node-admin executes; every read and command is Node RPC. Beside it: the Admin UI
(`demo/admin-ui`, `rafka-node-admin-client` → node-admin's HTTP API → a Build) and two consumer cells
(`demo/tests/app_channels.rs`, `demo/tests/connections.rs`) that compose their own small fabrics on the
public seams (`start_with(cfg, Wiring { .. })`, `start_with_rafka_time_before_ready_alpns_and`).

## 2. The matrix

| i141 / i142 need | demo evidence at `1feb0925` | status |
|---|---|---|
| **e1** one transport, logical FabricId vs EndpointId | the estate's twenty nodes, 2 meshes, one iroh endpoint per process; ids canonical (`mock_estate_forms_twenty_distinct_nodes`, composition-process ok 101 s, -container ok) | demonstrated |
| **e2** connections model, no duplicate route truth; **e12** the data plane through `Node::connections(alpn)` with `Alpn { forwardable, carriers }` | `demo/tests/connections.rs` (14 cells, release job `demo-rshape-consumer-connections` ok 121 s): Direct/Proxy per ALPN, false-proxy refused until the exact birth answers, tunnel stream reply loss replays nothing, per-ALPN Direct restore leaves another ALPN's proxy intact, concurrent streams through the carrier keep pairing/finish/reset/stop codes, source restart resumes the right ALPN from the persisted next-due; routing chaos `mock_routes_survive_connection_and_birth_supersession`, `mock_proxy_cutback_requires_durable_retirement`, `mock_connection_churn_releases_superseded_resources` (process + container, ok); faults only through `rdm_test::connection_faults` (Q62) | demonstrated |
| **e3** shape → complete-topology Builds, the fabric-primary-only Build call, `Fabric.build_id` | Admin UI: `every_ui_topology_flow_is_one_build_in_node_admin` (spawn, restart, remove, bootstrap each one Build), `node_admin_refusals_reach_the_ui_with_their_reason`, `without_a_node_admin_the_ui_says_so_by_name` (`demo-rafka-admin-ui-*` ok); estate: every birth by `build.create.via-node-rpc` → `node.create.via-build` | demonstrated |
| **e3** Rafka production backends on RDM's six storage boundaries (`StorageBackend`) | the demo's node-admin runs RDM's own file backend (`entry::run`); the seam is proven by RDM's `rdm-test memory_estate` (one admin on `MemoryBackend`: birth, node.create ×2, stop, start, restart, build.get) and `entry::run_on(service, storage, wiring)` exists for a consumer | RDM-proven; **the demo binds no foreign backend** (i141 e3 s6/s8, rafka-v2's) |
| **e3** DeploymentProvider / provider-domain binding | `MESH_SPAWN_TYPE=process` and `=container` layers of every R-shape job; container proof and `container_kill` in the release | demonstrated |
| **e4** exact-runtime adoption, RuntimeFact publication, successor adoption | `mock_recovery_retains_build_and_advances_attempt`, `mock_control_loss_recovers_without_status_heartbeat`, `mock_node_replacement_creates_distinct_logical_identity` (authority/lifecycle chaos, process + container, ok); §18.4 process + Docker-domain arms through those layers | demonstrated (same host; multi-host not claimed, as the import manifest states) |
| **e4** lifecycle hooks, `before_ready`, Pending certainty | `demo/tests/app_channels.rs`: the broker's `before_ready` pull (`BeforeReadyPull` through node-admin's `serve_app`), live-vs-pull reconciliation under one comparator, round actions (`set_round_actions`: commit-state, open-traffic) | demonstrated at 2-node scale in the consumer cell; the estate's roles pass `BeforeReadyHook::none()` |
| **e4** Day-0 hooks, bootstrap → durable control-state promotion (`FabricHooks::sync_state`) | the demo passes `FabricHooks::no_app_work()` everywhere; RDM's own `fabric/rounds.rs` cells prove the hooks | RDM-proven; **not demonstrated by the demo** (rafka-v2 e4 s2-s5) |
| **e6** real cohorts consume imported leadership: one-admin bootstrap, own-mesh Pending executor, active-Build failover (§18.1), post-completion drift (§18.2), proven-loss recovery, partition/heal (§18.3), endpoint movement | `mock_authority_handoffs_preserve_one_effective_writer`, `mock_recovery_retains_build_and_advances_attempt`, `mock_partition_heal_recovers_coverage_and_routes` (real `nft`, `RDM_REQUIRE_NETFAULT=1`), `mock_role_restarts_preserve_identity_and_birth`, the adv-cut cells (near/far side under a cut; restart, kill, spawn, two victims, primary victim, direct-edge loss declares no death, carrier-edge loss retires the proxy within budget) | demonstrated |
| **e7** one sealed catalog, Node RPC certainty and pool semantics | `mock_traffic_accounts_for_every_outcome_without_uncertain_replay`, `mock_roles_route_opaque_calls_to_exact_nodes`; connections.rs's reply-loss cell; RDM's `certainty.rs`, `pool.rs`, `forward.rs` cells (§14) | demonstrated (outcomes), RDM-proven (the wire) |
| **e8** seeded restart/product acceptance with OTLP evidence | every R-shape cell writes `result.json`, the estate manifest, the rpc ledger and every process's spans under `RDM_ARTIFACTS_DIR`; soaks seeded (`RDM_RSHAPE_SEED`) | demonstrated |
| **e9** the load proof (§19) | none in the demo; R18 is the soaks | **gap**, rafka-v2 e9 (and `lane-queue/_perf-throughput-plan.md`) |
| **e11** entity gossip only through `Membership::join_channel` / `Channel` / `Event` / `Received`; node-admin holds every channel as the app | `app_channels.rs`: two declared types, publish/receive, the two loss shapes (queue overflow healed by the pull; incomplete assembly evicted at the staleness floor), node-admin holding both channels; Q84: the join without a dropped neighbour, flame on .99 | demonstrated at 2-node scale; the twenty-node estate declares no app type (its node-admin's `Wiring::types` is empty) |
| **e10 / e10.s2** export SHA, parity-through, manifest | `export-manifest --candidate 1feb0925… --repo /home/admin/rafka-v2`: parity clean, 2 gate logs certified, 81 §14 cells proven; receipts being written by the export run and the soaks at this SHA | in progress at `1feb0925` |
| **i142** the entry pull as a Rafka projection op through node-admin's `serve_app` | `app_channels.rs`: `serve_app: Some(serve)` answering `PullRequest` → `PullReply` rows, applied under the app's comparator | demonstrated (the mechanism; the Rafka op is i142's) |
| **i142** `epoch` → `revision`, the drain/linger removal | RDM side landed (`3eaae3e6`, `d9f8235f`); the demo uses neither word | done on RDM's side |
| **i142** Forward-tag migration, pool identity, writer certainty budgets | RDM's `forward.rs`, `pool.rs`, `certainty.rs` cells (§14); demo: tunnel reply loss, cutback within the caller's budget | RDM-proven; demonstrated in part |

## 3. Verdict

The demo demonstrates, through the public API alone, every mechanism rafka-v2 binds in i141 e1, e2, e3's
Build path, e4's runtime adoption, e6, e7's outcomes, e8, e11 and e12, and the i142 entry-pull mechanism, at
twenty nodes under process and container providers with real network cuts, at one SHA whose release suite is
green. It is ready for ingestion on those rows.

Three things it does not demonstrate, and the record should not imply it does:
1. **A consumer-bound storage backend** in a running estate (i141 e3 s6/s8): the seam (`StorageBackend`,
   `entry::run_on`) exists and RDM's `memory_estate` cell proves it; the demo's node-admin runs the file backend.
2. **The app-embedding shape of node-admin at estate scale**: declared `types`, `alpns`, `before_ready`,
   `FabricHooks::sync_state`, `round_actions` are exercised in the two consumer cells (2-3 nodes), not in the
   twenty-node estate, whose roles are the testkit's proof families.
3. **The load proof** (i141 e9): nothing in the demo measures throughput.

Both 1 and 2 are a consumer's bindings (rafka-v2's rows), and the seams they bind are proven here; 3 is
rafka-v2 e9. None blocks the export; each is named so i141 does not discover it as a surprise.

Two corrections for the import manifest at export: its fork line names iroh-gossip `dce7b706`, the pin is
`508fb795` (`Cargo.toml:53`, the back-out Luke permitted, Q75/Q77); and the "Rafka-shaped qualification … not
run at an export SHA" red row closes with this export run and the soaks at `1feb0925`.
