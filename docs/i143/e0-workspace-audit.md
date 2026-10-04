# i143 e0 — RDM workspace audit and legacy-binary disposition

Story: drlukeangel/rafka-v2#2731 (i143.e0.s1). Plan: rafka-v2
`docs/plans/i143-node-rpc-pos-on-RDM.md` §4 (package architecture), §17
(RDM role binaries), gap rows R21 and R4/R7/R18.

This doc states what the RDM workspace holds at the opening of i143 and where
each piece goes in the PRD §4 target packages. The disposition table is the
machine-checked record: `cargo test -p rafka-mesh-audit --test legacy_binaries`
fails if any legacy binary has no row, two rows, an unknown disposition, or a
disposition the tree contradicts. The same test fails if any proof-shape
declaration (scenario YAML under `crates/*/scenarios/`, or a node-admin
`shape*`/`topology.rs` source) names a legacy role.

## Legacy binary dispositions

| binary | disposition | why |
|---|---|---|
| `broker` | retained-example | 43-line wrapper: `NodeRuntime::new("broker").with_role(Role::Broker)`. `Role::Broker` has no behavior of its own in `rafka-node-base`. Kept as a legacy demo program for the legacy Admin UI (`KNOWN_NODE_TYPES`, `/api/bootstrap`), `rafka-chaos` soak targets and `rfa`. Outside the import manifest; never a proof-shape node. |
| `gateway` | retained-example | Same wrapper as `broker` with `Role::Gateway` (no behavior). Same consumers, same status. |
| `compute` | retained-example | Same wrapper with `Role::Compute` (no behavior). Same consumers, same status. |
| `registry` | retained-example | Same wrapper with `Role::Registry` (no behavior). Same consumers, same status. |
| `bridge` | extraction-input | `Role::Bridge` is the only role with behavior: it joins several mesh ids (`RAFKA_BRIDGE_TARGET_MESHES`), records each peer's mesh from the `Hello` frame (`MeshIdRegistry`) and emits per-peer-mesh aggregates. That peer→mesh association is generic multi-mesh membership input for the Mesh EF (e4: exact-node identity/membership, cross-mesh reachability). The binary itself stays a legacy demo until e4 owns the behavior. |

No legacy binary is dead: each is reachable from the legacy Admin UI spawn
path, the chaos soak and the `rfa` CLI. None is deleted in e0. The retained
programs leave the workspace when their last consumer does — the Admin UI
becomes a node-admin client in e1, and the chaos crate moves to public
control/probe interfaces in e8 — and each removal edits this table to
`dead-deleted` in the same commit (the gate then requires the crate to be
gone).

The i143 proof estate (SN/MN/MM) is built from generic node-admins and generic
RPC proof nodes only (PRD §1 decision 13, ownership §17).

## Workspace survey against the PRD §4 target packages

| current crate | what it holds | target package(s) | gap row |
|---|---|---|---|
| `crates/rafka-mesh-transport` (97 lines) | `IrohMeshTransport::new`: iroh endpoint, ALPN `rafka-mesh-v1` + gossip ALPN, relay disabled, 15 s keep-alive / 30 s idle, optional mDNS lookup; `await_disconnect` reason mapping. | `rafka-mesh-transport` (kept; generic). Identity load/mint and endpoint creation in node-base move here or to `rafka-node-rpc`. | R3 |
| `crates/rafka-mesh-ops` (276 lines) | Framer `tag + varint(len) + postcard`; `TAG_LEGACY_FRAME = 0x10`; `InternalMeshFrame { Ping{org_id}, Pong{org_id}, Hello{mesh_id,node_type} }` with OTel trace-context carriage. | Framing → `rafka-node-rpc-contract` (tag-first read, per-protocol ceiling, 421–424/499 namespace). `org_id` on Ping/Pong is legacy Rafka vocabulary and does not enter the contract. | R1, R14, R15 |
| `crates/rafka-node-base` (2,651 lines) | `NodeRuntime` main loop: identity file, endpoint, gossip digest broadcast (`GossipDigest`), process-global `live_digests`/`topic_membership` maps with a staleness pruner, seed dial, mDNS watch, accept loop (gossip vs mesh ALPN), uni-stream frame reader, `TAG_BI_ECHO = 0x11` bi-stream echo (unknown tags are dropped with a span, not reset), heartbeat/load sampling, `Deployment` gate for `RAFKA_DEV_*`. | Membership/digest → `rafka-mesh-entity`. Bi-stream dispatch/echo → `rafka-node-rpc` (core Echo stays 0x11). Unknown-tag drop becomes canonical `421 UNSERVED_TAG`. No incarnation id, no endpoint-slot freshness, no pool. | R1, R2, R3, R14 |
| `admin-ui` (4,437 lines, axum) | The only lifecycle authority today: `spawn_one`/`kill_one` over a process map (`processes`, `spawned_meta`), `/api/nodes/spawn`, `DELETE /api/nodes/{name}`, `/api/bootstrap` (fixed 2-mesh legacy role set), chaos loop, topology/alerts/timeline reads, observer node. Spawn dirs hardcode `E:/tmp/rafka-ui-nodes/<name>`; ports are OS-ephemeral (no advertised-endpoint authority); no Build, no build id, no restart route. Also holds the dead `_HTML_LEGACY_REMOVED` string. | Lifecycle → `rafka-node-admin-core` (`model`, `topology`, `build`, `build_state`, `http`) + `rafka-node-admin-client`; launch → `deployment::{provider,pipeline,process,container,endpoint,receipt}`. The Admin UI becomes a client of node-admin core. | R4, R5, R6, R7, R8 |
| `crates/rafka-chaos` (1,728 lines) | `ChaosPrimitive` trait; Kill/Restart/BurstKill go through Admin UI HTTP (`/api/nodes/spawned`, `DELETE /api/nodes/{name}`, `/api/nodes/spawn`). Wedge = OS suspend (`pgrep`/`kill -STOP` on unix, PowerShell on Windows). Partition/Subset/FlapLink/FirewallInbound are PowerShell-only (Windows firewall). ClockSkew/NatShift respawn with env. Soak driver counts detections, not semantic outcomes. | `rafka-chaos` (kept). Needs: public control/probe interfaces instead of Admin UI globals, container/network backend on Linux, failpoints, deterministic network/time, semantic wedge detectors. | R17, R18, R19, R20 |
| `crates/rafka-telemetry` (166 lines) | `init_telemetry(service)` OTLP/gRPC exporter. | Shared by every package; e7 adds an OTLP JSONL evidence sink. | R16 |
| `cli/rfa` (1,708 lines) | Operator CLI over the Admin UI HTTP surface; chaos primitive shortcuts by legacy role. | Becomes a `rafka-node-admin-client` consumer after e1. | — |
| `broker`/`gateway`/`compute`/`registry`/`bridge` | See the disposition table above. | — | R21 |

Packages absent today: `rafka-node-rpc-contract`, `rafka-mesh-entity`,
`rafka-node-rpc`, `rafka-node-admin-core`, `rafka-node-admin-client`,
`rafka-test-scenario`, `rafka-node-rpc-testkit`.

## Build hygiene found during the audit

The workspace `Cargo.toml` carried a `[patch.crates-io]` block pointing at
Windows-only paths (`E:/iroh/...`, `E:/iroh-gossip`, `E:/noq/...`). On any
other host `cargo` refused to load the workspace at all. The block now lives in
`deployment/dev/windows-iroh-patches.toml`, opted into per host with
`cargo --config deployment/dev/windows-iroh-patches.toml ...`; the workspace
builds and tests against crates.io `iroh 1.0.0-rc.1` / `iroh-gossip 0.100.0`
everywhere else.
