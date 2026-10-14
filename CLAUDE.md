# CLAUDE.md — RDM agent instructions

RDM (rust-distributed-mesh) is the generic Mesh product: the fabric, mesh and node runtime
(node-admin, Node RPC, gossip, Build, deployment providers) that rafka-v2 pins as a dependency and
consumes. Everything rafka-specific (gateways, brokers, orgs, topics, ACLs) lives in rafka-v2 and
is never built here.

## The spec and the record

- **The spec is `docs/architecture/`** and its `lifecycles/` pages. A page is the contract; code
  conforms to the page, never the other way round. `docs/architecture/fabric-mesh-node-lifecycles.md`
  is the index. No agent changes an architecture page without Luke's yes on the exact old and new
  text.
- **The record is `docs/plans/i143-prd.md`**, the only source of truth for remaining work: every
  state or remaining-work update goes in its row. Rulings are in `docs/plans/rulings/`, proposals
  in `docs/plans/proposal/`. Issues and the i143 milestone are tracked in the rafka-v2 GitHub repo.
- **The canon in `/home/admin/rafka-v2/CLAUDE.md` applies here in full**: no change to gossip or the
  mesh without Luke's permission and a Jaeger flame; fix the root, never a coping layer; commands go
  down (fabric-primary → mesh-primary → node); the vocabulary bans; the durable-format rule and its
  commit marker; no Claude attribution in commit messages.

## Vocabulary

`id` / `index` / `name` are three things; `key` is none of them. `target`, never fence. `revision`,
never epoch. Connections are deleted (`node.connections.delete`), never drained. `forward` is the
control plane's word (Forward 0x1A, `build.attempt.run`); `proxy` is the data plane's (a `Proxy`
connection). Banned everywhere: `relay`, `cluster`, `tailer`, `sidecar`, `child mesh`, `viewer`,
`operator`, `sysadmin`, `tree`, `partition`, `reborn`. A node is restarted (same node.id and
incarnation) or re-born (same path.name, new node.id); a mesh is never reborn.

## Lanes, gate, landing

- A lane is a folder with a source tree: `/home/admin/rdm-lanes/<name>`. Reuse it (rebase on
  `origin/main`, new branch); never a new worktree, never `.claude/worktrees`, never delete one.
  `/home/admin/rust-distributed-mesh` holds `main` and is not a lane.
- Never export `CARGO_TARGET_DIR`; never `cargo test -p` in a lane (each `-p` shape rebuilds
  admin-core). One stem: `cells.sh`; the whole suite: `rdm-gate-at.sh <lane> fast|release`;
  landing: `rebase-gate-land.sh <lane> <branch>` (rebase, name check, gate, fast-forward push).
  The lead's copies are in `/home/admin/lane-queue/i143-lead/`.
- A red gate is read, never re-rolled. A landing that adds a retired word to an identifier or a
  file name, or an issue-named test file, is refused by the name check.
- Every landing that changes a page's statement lands the page text and the code together.

## Env vars (all node binaries)

All env vars recognized by node binaries (`gateway`, `broker`, `compute`, `registry`). No other configuration mechanism exists.

| Env var | Default | Description |
|---|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4318` | OTLP/HTTP collector base URL; spans go to `<url>/v1/traces` and log records to `<url>/v1/logs`, both protobuf. Override for any other collector. |
| `OTEL_SERVICE_NAME` | `gateway` | Service name shown in Jaeger's left-rail filter. |
| `RDM_DATA_DIR` | `./data/node-<random-hex>` | Directory where `node-identity.json` is stored. Set this to a stable path across restarts to preserve node identity (same `node_id` across reboots). |
| `RDM_NODE_BIND_ADDR` | `0.0.0.0:0` | IPv4 socket address iroh binds the QUIC endpoint to. Port 0 = ephemeral OS-assigned. Override to pin to a specific port for firewall rules. |
| `RDM_GOSSIP_INTERVAL_MS` | `2000` | Gossip heartbeat interval in milliseconds. Bumped from 500 → 2000 as part of the CPU-optimization pass (07-cpu-optimization-plan.md): substrate health doesn't need 2 Hz granularity, and at 18 nodes the per-tick fanout cost was non-trivial. |
| `RDM_STALENESS_MS` | `30000` | Background pruner threshold (milliseconds). A `GossipDigest` whose `wall_time_ms` is older than this is removed from the process-global `live_digests` + `topic_membership` maps. Sweeps every 5 seconds. At the default 2s gossip cadence, 30s = 15 missed cycles. Override to tune how aggressively stale peers are evicted from the topology view. |
| `RDM_SEED_NODES` | _(empty)_ | Comma-separated list of `<node_id_hex>@<host>:<port>` entries to dial on boot. Each seed triggers `rdm.mesh.peer.discovered` + `rdm.mesh.peer.connected` spans. Example: `abc123...@127.0.0.1:14820`. Added Sprint 03. |
| `RDM_AUTO_SHUTDOWN_SECS` | _(unset = wait for signal)_ | If set, node shuts down cleanly after this many seconds. Verification hook only — used to produce a clean process exit (and thus flush OTLP spans) in environments where Ctrl+C delivery is unreliable (e.g. Windows child process). |
| `RDM_TOPOLOGY_UI_BIND_ADDR` | `127.0.0.1:19090` | TCP address the `rafka-topology-ui` HTTP server binds to. Override to expose on a different interface or port. |
| `RDM_NODE_ADMIN_API_BASE` | _(unset)_ | The node-admin control API the Admin UI drives (the URL node-admin prints). The UI submits every topology change there as a Build through `rafka-node-admin-client`; unset, its spawn/restart/remove/bootstrap routes answer `503 no-node-admin`. |
| `JAEGER_QUERY_URL` | `http://localhost:16686` | Base URL of the Jaeger Query API. Used by `rafka-topology-ui` (chunk 2+) to fetch trace data for the boot-waterfall panel. |
| `CARGO_TARGET_DIR` | `./target` | Read by `rafka-topology-ui` to locate node binaries for spawn. Set to `E:/cargo-target-sprint-02` in local dev so the UI can find debug builds without a separate install step. |
| `RDM_DEPLOYMENT` | `prod` | One of `dev`, `staging`, `prod`. Gates all `RDM_DEV_*` overrides. Defaults to `prod` so a production manifest gets safe behavior without explicit setting. |
| `RDM_DEV_CPU_BUDGET` | _(measured via sysinfo)_ | Override the reported `cpu_budget` field in `GossipDigest` (cores, fractional). Honored only when `RDM_DEPLOYMENT != prod`. Used by per-crate `.env.dev` to give heterogeneous test brokers different CPU profiles. |
| `RDM_DEV_RAM_BUDGET` | _(measured via sysinfo)_ | Override reported `ram_budget` in GB. Same gating as `RDM_DEV_CPU_BUDGET`. |
| `RDM_DEV_CPU_USED` | _(measured via sysinfo)_ | Override reported `cpu_used` in cores. For deterministic routing/migration test scenarios (broker reports 95% load without actually being loaded). Same gating. |
| `RDM_DEV_RAM_USED` | _(measured via sysinfo)_ | Override reported `ram_used` in GB. Same gating. |
| `RDM_CPU_ALERT_THRESHOLD` | `0.10` | Cores. Admin-ui `/api/alerts` emits a warn-severity alert for any node whose latest `GossipDigest.cpu_used` exceeds this. Release-build empty-shell baseline is ~0.02 cores; default 0.10 = ~5× headroom. Read once per `/api/alerts` request. |
| `RDM_RAM_ALERT_THRESHOLD_GB` | `0.5` | GB. Same shape as `RDM_CPU_ALERT_THRESHOLD` but for `ram_used`. Release baseline ~0.06 GB; default 0.5 = ~8× headroom. |
| `MESH_SPAWN_TYPE` | `process` | `process` or `container`: the deployment provider. Read once by the first node-admin and fixed as fabric policy; later admins take it from their entry pull, an unknown value refuses startup, and a Build never names a provider. |
| `RDM_CONTAINER_SUBNET_POOL` | `10.231.0.0/16` | Container provider: the IPv4 block each fabric's bridge network (`rafka-<fabric>`) takes a `/24` from; node-admin picks the subnet so it can assign every node's address (`--ip`). A `/24` another network uses is skipped. |
| `RDM_REQUIRE_CONTAINER` | _(unset)_ | Tests only. `1` turns the container smoke's named skip on an unsupported host into a failure (set in CI). |
| `RDM_REQUIRE_NETFAULT` | _(unset)_ | Tests only. `1` turns the election E2E's named skip of its partition case (no `iptables`, as root or through `sudo -n`) into a failure (set in CI). |
| `RDM_EVIDENCE_DIR` | _(unset = no JSONL)_ | When set, every i143 binary writes its spans as JSONL to `<dir>/<service>.<pid>.spans.jsonl` (the blackbox evidence). OTLP export stays on `OTEL_EXPORTER_OTLP_ENDPOINT`. |
| `RDM_FABRIC` | `fabric1` (bootstrap admin); required for a launched node | The name (label) of the fabric a node joins. The first `rafka-node-admin` reads it to name the fabric it bootstraps; for every node a pipeline launches, node-admin writes it. An empty value is refused. |
| `RDM_FABRIC_ID` | _(minted by the bootstrap admin)_; required for a launched node | The Fabric's identity (canonical Crockford60: 12 lowercase Crockford characters); its gossip membership, backbone and Build topics are keyed by it. The bootstrap admin mints it unless given; for every node a pipeline launches, node-admin writes it. A non-canonical value is refused by name. |
| `RDM_NODE_NAME` / `RDM_NODE_ID` / `RDM_INCARNATION_ID` | _(required for a launched node)_ | The node's `path.name` (`mesh1.rpc.2`, `mesh1.admin.2`), minted node id (canonical Crockford60, refused by name otherwise) and this birth's incarnation id, assigned by node-admin. A `rafka-node-admin` without them is the bootstrap admin of its fabric. |
| `RDM_SUPERSEDES` | _(unset = first birth)_ | The incarnation a restart replaces. |
| `RDM_ENDPOINTS` | _(required, rpc node)_ | Comma-separated `slot=addr=freshness` the node must bind exactly; a provider never invents a port. |
| `RDM_SEEDS` | _(empty)_ | Comma-separated `<public key>@<addr>` gossip members to join membership through. |
| `RDM_MESH` | `mesh1` | `rafka-node-admin` (bootstrap): the mesh it belongs to; it becomes `<mesh>.admin.1`. |
| `RDM_MESH_ID` | _(minted)_ | The mesh's id (canonical Crockford60, refused by name otherwise), written by the launching admin for every node it launches so every node of a mesh carries the same one. |
| `RDM_NODE_ADMIN_API_BIND` | `127.0.0.1:0` | `rafka-node-admin` (bootstrap): the control API's HTTP bind. A launched admin binds the `control` slot node-admin assigned. It prints `RDM_NODE_ADMIN_API_BASE=<url>` once serving. |
| `RDM_BIN_DIR` | _(beside the running exe)_ | `rafka-node-admin`: where `rafka-node-admin` and `rafka-rpc-node` live. |
| `TRACEPARENT` | _(unset)_ | W3C traceparent of the deploying step; the node's boot span `rdm.mesh.node.create.via-deployment` parents to it. |
| `RDM_DRAIN_DEADLINE_MS` | `5000` | rpc node: on SIGTERM, how long in-flight handlers may finish before the node leaves. |
| `RDM_LEAVE_LINGER_MS` | `1000` | Every node (rpc node and node-admin): how long a stopping node keeps announcing `Leaving` on membership (every 200 ms) before it closes (iroh-gossip acknowledges nothing; closing drops unsent data). Drain deadline plus linger stay inside node-admin's stop grace (8 s). |

**Infrastructure context (Sprint 01):** The shared `rafka-test-otel-collector` receives OTLP/HTTP on `localhost:4318`; the nodes' exporter posts spans and log records there. Jaeger UI: `http://localhost:16686`.

---
