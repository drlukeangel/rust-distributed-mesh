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

The primaries are the fabric-primary, each mesh's mesh-primary and each node type's node-type primary in its mesh; a primary changes by `node.primary.update` or, unheard past the staleness floor, by the leadership election. `id` / `index` / `name` are three things; `key` is none of them. `target`, never fence. `revision`,
never epoch. Connections are deleted (`node.connections.delete`), never drained. `forward` is the
control plane's word (Forward 0x1A, `build.attempt.run`); `proxy` is the data plane's (a `Proxy`
connection). Banned everywhere: `relay`, `cluster`, `tailer`, `sidecar`, `child mesh`, `viewer`,
`operator`, `sysadmin`, `tree`, `partition`, `reborn`, `drain` / `draining` / `drained` (the state is
`connections-deleting`, the bound `RDM_CONNECTIONS_DELETE_DEADLINE_MS`: [node-connections-delete](docs/architecture/lifecycles/node-connections-delete.md)),
`park` / `parked`, `fence`, `epoch`, `relaunch`, `departed`, `retire` / `retired`, `seat` / `seated` / `unseat`, `holder` (of a primary; a name's holder in `NameHeld` is another sense), `authority` / `authorize` / `authoritative`, `cohort`, `lane`, `cut-off` / `cut off`, `committed seat`, `exit proof` / `proven exit`, `held-running`, `hung hold`, `LeadershipView` / leadership `resolver`, `sticky`, `roll` / `rolls` / `rolled` / `rolling` (the operation is a node's executable replacement, a Build; `docs/architecture/build-deployment.md`), `climb` / `climbs` / `climbed` / `climbing`, `round` / `rounds` (the fabric has states, pending, state-sync, state-commit, ready-for-traffic; a state is set fabric-wide and is set when every member has checked in; the whole movement from pending to ready-for-traffic is the fabric's change) (the name check's set,
`scripts/name-check.py`). A node is restarted (same node.id and
incarnation) or re-born (same path.name, new node.id); a mesh is never reborn.

## Rulings

A ruling is asked one at a time, with a recommendation, and is produced with the `ruling-packet`
skill (`/home/admin/rafka-v2/.claude/skills/ruling-packet`): spec, issue, flame, code, delta, arms. A
packet without a section 5 delta is returned, not reviewed. Every question and ruling carries a number.

## Lanes, gate, landing

- A lane is a folder with a source tree: `/home/admin/rdm-lanes/<name>`. Reuse it (rebase on
  `origin/main`, new branch); never a new worktree, never `.claude/worktrees`, never delete one.
  `/home/admin/rust-distributed-mesh` holds `main` and is not a lane.
- Never export `CARGO_TARGET_DIR`; never `cargo test -p` in a lane (each `-p` shape rebuilds
  admin-core). One stem: `scripts/cells.sh`; the whole suite: `scripts/rdm-gate-at.sh <lane>
  fast|release`; landing: `rebase-gate-land.sh <lane> <branch>` (rebase, name check, gate,
  fast-forward push). Every script, with the helpers it calls (`name-check.py`, `timings_row.py`),
  lives in this repo's one `scripts/` folder: the land script runs main's copy, the gate runs the
  lane's own copy, and `cells.sh` builds exactly what the gate builds. The two files in
  `/home/admin/lane-queue/i143-lead/` are four-line shims, not sources: `rebase-gate-land.sh` runs
  origin/main's `scripts/rebase-gate-land.sh` and `rdm-gate-at.sh` runs the lane's
  `scripts/rdm-gate-at.sh`; a change to either goes in `scripts/`. Gate state (the red-tree list,
  the timings ledger) and the gate logs are in `/home/admin/rust-distributed-mesh-deploy/`.
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
| `RDM_ADMIN_UI_BIND_ADDR` | `127.0.0.1:19090` | TCP address the demo admin UI (`demo/admin-ui`) HTTP server binds to. Override to expose on a different interface or port. |
| `JAEGER_QUERY_URL` | `http://localhost:16686` | Base URL of the Jaeger Query API. Used by the demo admin UI (`demo/admin-ui`) to fetch trace data for the boot-waterfall panel. |
| `CARGO_TARGET_DIR` | `./target` | Read by the gate and cell scripts and the demo admin UI to locate node binaries. Never exported by hand in a lane (see Lanes). |
| `RDM_DEPLOYMENT` | `prod` | One of `dev`, `staging`, `prod`. Gates all `RDM_DEV_*` overrides. Defaults to `prod` so a production manifest gets safe behavior without explicit setting. |
| `RDM_DEV_CPU_BUDGET` | _(measured via sysinfo)_ | Override the reported `cpu_budget` field in `GossipDigest` (cores, fractional). Honored only when `RDM_DEPLOYMENT != prod`. Used by per-crate `.env.dev` to give heterogeneous test brokers different CPU profiles. |
| `RDM_DEV_RAM_BUDGET` | _(measured via sysinfo)_ | Override reported `ram_budget` in GB. Same gating as `RDM_DEV_CPU_BUDGET`. |
| `RDM_DEV_CPU_USED` | _(measured via sysinfo)_ | Override reported `cpu_used` in cores. For deterministic routing/migration test scenarios (broker reports 95% load without actually being loaded). Same gating. |
| `RDM_DEV_RAM_USED` | _(measured via sysinfo)_ | Override reported `ram_used` in GB. Same gating. |
| `RDM_CPU_ALERT_THRESHOLD` | `0.10` | Cores. Admin-ui `/api/alerts` emits a warn-severity alert for any node whose latest `GossipDigest.cpu_used` exceeds this. Release-build empty-shell baseline is ~0.02 cores; default 0.10 = ~5× headroom. Read once per `/api/alerts` request. |
| `RDM_RAM_ALERT_THRESHOLD_GB` | `0.5` | GB. Same shape as `RDM_CPU_ALERT_THRESHOLD` but for `ram_used`. Release baseline ~0.06 GB; default 0.5 = ~8× headroom. |
| `MESH_SPAWN_TYPE` | `process` | `process` or `container`: the deployment provider. Read once by the first node-admin and fixed as fabric policy, which it writes on the Fabric record; later admins take it from their entry pull, an adoption launch from the Fabric record (a differing value refused by name, Q156), an unknown value refuses startup, and a Build never names a provider. |
| `RDM_CONTAINER_SUBNET_POOL` | `10.231.0.0/16` | Container provider: the IPv4 block each fabric's bridge network (`rafka-<fabric>`) takes a `/24` from; node-admin picks the subnet so it can assign every node's address (`--ip`). A `/24` another network uses is skipped. |
| `RDM_REQUIRE_CONTAINER` | _(unset)_ | Tests only. `1` turns the container smoke's named skip on an unsupported host into a failure (set in CI). |
| `RDM_DEBUG_GOSSIP` | _(unset)_ | Tests only. When set, `EvidenceCapture` adds a stderr layer of iroh-gossip's own DEBUG lines (the dial, accept and close of every gossip connection), on one clock with RDM's spans: the tooling that read Q84's drop. |
| `RDM_REQUIRE_NETFAULT` | _(unset)_ | Tests only. `1` turns the election E2E's named skip of its partition case (no `nft`, as root or through `sudo -n`) into a failure (set in CI). |
| `RDM_EVIDENCE_DIR` | _(unset = no JSONL)_ | When set, every i143 binary writes its spans as JSONL to `<dir>/<service>.<pid>.spans.jsonl` (the blackbox evidence). OTLP export stays on `OTEL_EXPORTER_OTLP_ENDPOINT`. |
| `RDM_FABRIC` | `fabric1` (bootstrap admin); required for a launched node | The name (label) of the fabric a node joins. The first `rdm-node-admin` reads it to name the fabric it bootstraps; for every node a pipeline launches, node-admin writes it. An empty value is refused. |
| `RDM_FABRIC_ID` | _(minted by the bootstrap admin)_; required for a launched node | The Fabric's identity (canonical Crockford60: 12 lowercase Crockford characters); its gossip membership, backbone and Build channels are keyed by it. The bootstrap admin mints it unless given, and a given id whose store root already holds that Fabric's record is refused by name; an adoption launch (`RDM_NODE_NAME`) requires it, its store root holding that Fabric's record or the start refused by name naming the id and the root (Q156); for every node a pipeline launches, node-admin writes it. A non-canonical value is refused by name. |
| `RDM_NODE_NAME` / `RDM_NODE_ID` / `RDM_INCARNATION_ID` | _(required for a launched node)_ | The node's `path.name` (`mesh1.rpc.2`, `mesh1.admin.2`), minted node id (canonical Crockford60, refused by name otherwise) and this birth's incarnation id, assigned by node-admin. A `rdm-node-admin` given `RDM_NODE_NAME` with no `RDM_NODE_ID` and no `RDM_LAUNCHER` is the adoption launch (Q156; [fabric recovery](docs/architecture/lifecycles/fabric-recovery.md#nothing-alive) "Nothing alive"): the lost fabric-primary's path.name, with `RDM_FABRIC_ID`, `RDM_MESH` and `RDM_STORE_DIR` required and the root and CA from its environment or an inherited data dir; it mints its own node id and no fabric or mesh id. A `rdm-node-admin` without any of them is the bootstrap admin of its fabric (Day 0). |
| `RDM_SUPERSEDES` | _(unset = first birth)_ | The incarnation a restart replaces. |
| `RDM_TRANSPORT_ADDR` | _(required for a launched node)_ | The address the node's Iroh endpoint binds, assigned by node-admin with port 0: the operating system picks the port and the node reports where it bound at its join. |
| `RDM_LISTENERS` | _(written by the launching admin; empty for a kind that declares none)_ | Comma-separated `name=addr`: one entry per listener the application's wiring declares on the node's kind, each assigned the birth's address with port 0; a provider never invents a port. The node binds each, keeps the bind open through its join and reports the port the operating system selected at the join, which refuses a report not carrying exactly the assigned names on their addresses; the node row publishes them. |
| `RDM_SEEDS` | _(empty)_ | Comma-separated `<public key>@<addr>` gossip members to join membership through. |
| `RDM_MESH` | `mesh1` | `rdm-node-admin` (bootstrap): the mesh it belongs to; it becomes `<mesh>.admin.1`. An adoption launch requires it, the mesh of its `RDM_NODE_NAME`, and reads that mesh's MeshId from the store (Q156). |
| `RDM_MESH_ID` | _(minted)_ | The mesh's id (canonical Crockford60, refused by name otherwise), written by the launching admin for every node it launches so every node of a mesh carries the same one. |
| `RDM_NODE_ADMIN_API_BIND` | `127.0.0.1:0` | `rdm-node-admin` (bootstrap): its IP is the address the Day-0 admin's mesh transport binds, with port 0 (`boot.rs`, `cfg.api_bind`); a Day-0 admin in a container sets its container address here. No HTTP server is bound. Once serving, the admin prints its `rdm://<node id>` base as `RDM_NODE_ADMIN_API_BASE=<base>`. |
| `RDM_BIN_DIR` | _(beside the running exe)_ | rdm-node-admin: where rdm-node-admin itself is found; never a launch source: every created node's executable is the Build's. |
| RDM_EXECUTABLE_DIRS | _(unset)_ | rdm-node-admin under the container provider: comma-separated host directories mounted read-only at their host paths into a launched admin's container; an executable a Build names outside them is refused by name at acceptance. |
| `TRACEPARENT` | _(unset)_ | W3C traceparent of the deploying step; the node's boot span `rdm.mesh.node.create.via-deployment` parents to it. |
| `RDM_CONNECTIONS_DELETE_DEADLINE_MS` | `5000` | rpc node: the bound of a node-wide connections delete, on `node.connections.delete { All }` and on a signal stop alike ([node-connections-delete](docs/architecture/lifecycles/node-connections-delete.md)): how long in-flight handlers may finish before the node leaves. |
| `RDM_DATA_DIR_GONE_FLOORS` | `2` | `rdm-node-admin`: how many staleness floors (`RDM_STALENESS_MS`) a member whose node investigation verdict is `exited` and whose inherited `data_dir` is absent (`NotFound` on the directory itself) is held before the mesh-primary records `data-dir-lost { path, since }` and recovers it at the same `path.name` over a newly allocated `data_dir` with a newly minted endpoint key and `inherited = false` (`R-FM2`). |
| `RDM_CA_SECRET` | _(bootstrap: unset = minted on day 0; launched admin: written by the launching admin)_ | The fabric root. The bootstrap `rdm-node-admin` takes it from the app (`CertChoice::Ca`) or mints it; every launched node-admin is written it beside its mesh's issuing material, so any admin that becomes fabric-primary can mint and rotate. RDM keeps it as `ca-secret` beside the data dir, state it reads back on a restart, like `node-key`. A new root while the fabric runs is `fabric.root.update`. |
| `RDM_DECLARED_SECRETS` | _(written by the launching admin)_ | A launched `rdm-node-admin`'s custody document of the app's declared secrets (Q150): JSON, `{"v":1,"declared":[{"name","kinds","value"}]}`, so a value holding a separator, `=`, a newline or nothing round-trips; written beside `RDM_CA_SECRET` by a launcher that holds declared values, so whichever admin runs a create can launch the named kinds. A document that does not parse, names an unknown version or repeats a name is refused by name, echoing none of it. The bootstrap admin reads each declared name once from its own environment instead. Every admin keeps the values beside its data dir as `declared-secrets` (the same document) and reads them back on a start without a launcher, so a value never changes while the fabric runs; a changed value is a new fabric. A declared value is set only in the environment of the kinds its declaration names and is withheld from every process's inherited environment; it is never written to a row, a Build record, a receipt, the launch record or a span, and a spawn error or a runtime's last words are scrubbed of it and of RDM's own secrets. The container provider passes each secret as `-e K`, its value in the docker client's environment. A declared name that RDM sets or passes through (`RDM_*`, `TRACEPARENT`, `TRACESTATE`, the pass-through list), or a name declared twice, refuses the admin's start by name, naming the fabric's provider. |
| `TESTKIT_DECLARED_SECRET` | _(unset)_ | Tests only. The testkit node-admin's (`faulted-node-admin`) declared secret, set on the broker and the gateway: a cell sets its value in the bootstrap admin's environment and reads which processes hold it. |
| `RDM_ISSUING_KEY` / `RDM_ISSUING_CERT` | _(written by the launching admin)_ | A launched `rdm-node-admin`'s mesh issuing material, beside the root and the fabric CA, minted under the fabric CA by the fabric-primary at the mesh's acceptance (`mesh.issuer.create`) and re-minted by `mesh.issuer.renew`. |
| `RDM_FABRIC_CA_KEY` | _(written by the launching admin)_ | The fabric CA's key: a launched `rdm-node-admin` is written it beside the root and its mesh's issuing material, whose cert carries the CA's cert. Every admin keeps its CA as `fabric-ca` beside the data dir and reads it back on a restart, the way it reads `ca-secret`; the bootstrap admin mints the CA on day 0 (`fabric.ca.create`). Issuing material without it is refused by name. |
| `RDM_MEMBER_CERT_TTL_SECS` | `86400` | Fabric policy (read once by the bootstrap admin, carried in the entry pull): a member cert's TTL. A member cert never outlives its issuer. |
| `RDM_ISSUING_CERT_TTL_SECS` | `2592000` | Fabric policy: a mesh issuing cert's TTL (30 days). |
| `RDM_CA_CERT_TTL_SECS` | `31536000` | Fabric policy: RDM's CA TTL (365 days); `fabric.ca.renew` rotates it internally at `RDM_CERT_RENEW_AT` under the same root. The root (the app's anchor) has no policy TTL: it rotates only on `fabric.root.update`. |
| `RDM_ROOTS` | _(written by the launching admin)_ | A launched node's trusted root set (one or two public keys), so a node born mid-rotation trusts both. |
| `RDM_CERT_RENEW_AT` | `0.67` | Fabric policy: the fraction of a cert's TTL after which its issuer renews it (`node.cert.renew`, `mesh.issuer.renew`), with a fixed jitter of up to 5 % of the TTL. |
| `RDM_INHERITED_DATA_DIR` | `0` | Written by `rdm-node-admin` into a launched node's environment (`1` when created over an existing `data_dir`, `0` when created over a newly allocated `data_dir`), surfaced to `before_ready` as `BeforeReadyCtx::inherited` (`R-FM2`). |

**Infrastructure context (Sprint 01):** The shared OTLP collector receives OTLP/HTTP on `localhost:4318`; the nodes' exporter posts spans and log records there. Jaeger UI: `http://localhost:16686`.

---
