# i143 — the generic Mesh product in RDM: working design

Plan: rafka-v2 `docs/plans/i143-node-rpc-pos-on-RDM.md` (the PRD). Architecture:
rafka-v2 `docs/architecture/{node-rpc-rdm-ownership,mesh-control-plane,node-rpc,connections}.md`.
This doc states the concrete RDM contracts the stories build to: binaries, the control API,
the node view, the probe, the evidence files and the span names. Where the PRD is silent this doc
decides; where they differ the PRD wins and this doc is corrected.

## 1. Packages and binaries

| package | owns | binary |
|---|---|---|
| `rafka-node-rpc-contract` | framing, `RpcOutcome`, reply classes, reserved codes, catalog/seal, certainty rules; no Iroh | — |
| `rafka-mesh-entity` | Mesh EF: logical node id, runtime incarnation, endpoint slots + freshness tokens, membership; imported connections model | — |
| `rafka-node-rpc` | runtime over `rafka-mesh-transport`: server dispatch, client commit cut, admission, slot-aware pool, streaming, one-hop carried execution | — |
| `rafka-node-admin-core` | Fabric/Mesh/Node model, Build, `BuildStateAdapter`, deployment providers/pipelines, lifecycle transitions, elections, HTTP | `rafka-node-admin` |
| `rafka-node-admin-client` | typed DTOs + HTTP client for the control API | — |
| `rafka-node-rpc-testkit` | Echo + stateful proof protocol/store, probes | `rafka-rpc-node`, `rafka-rpc-probe` |
| `rafka-test-scenario` | scenario runner, evidence/replay manifests, the e2e canaries | `rafka-scenario` |
| `rafka-chaos` | hostile layers: generator/shrinker, failpoints, simulated network/time, real fault backends | — |

Proof shapes use exactly two node kinds: `node_admin` and `rpc_node`. Legacy role binaries are never
proof-shape nodes (`docs/i143/e0-workspace-audit.md`).

## 2. Names and identities

| fact | form | survives restart | survives replacement |
|---|---|---|---|
| `path.name` | `<mesh>.<kind>.<ordinal>`, kind `admin` or `rpc`: `mesh1.rpc.2`, `mesh1.admin.1` | yes | yes (points at the replacement) |
| `node_id` | logical node identity minted at `AllocateIdentity`, opaque | yes | no |
| `fabric_id` | the node's Iroh key, kept in its data dir | yes | no |
| `incarnation_id` | minted by node-admin for every process birth, opaque | no | no |
| endpoint slot | `{slot, addr, freshness}`; freshness is an opaque token minted with each slot assignment | per slot policy | no |

Freshness and incarnation tokens are compared by equality/supersession only, never ordered.

An `rpc_node` has two Node RPC endpoint slots:

- `rpc-0`, policy `fresh`: every process birth gets a newly allocated port and a new token;
- `rpc-1`, policy `stable`: a restart keeps the advertised port and its token, and a replacement gets
  new ones.

A restart therefore always moves exactly one slot. That makes the per-slot supersession rules (PRD §1.19,
§14) observable on every restart.

### 2.0 Address authority (e4.s13)

Every Iroh endpoint is built from `presets::Minimal` with relay off. The addresses a node dials come from
exactly two places: node-admin's slot assignment (`RAFKA_ENDPOINTS`, the topology projection, the entry
pull) and the explicit seeds that bootstrap gossip (`RAFKA_SEEDS`); each channel registers them in a
`MemoryLookup`. No n0 DNS/Pkarr publication or resolution is configured anywhere (`presets::N0DisableRelay`
applies `N0`, n0 DNS/Pkarr included, before it disables relay). Local mDNS exists only on the legacy
`IrohMeshTransport` plane, as address discovery; no canonical i143 crate uses it, and nothing marks a node
dead from it. The `address-lookup` gate (`tools/mesh-audit`) enforces both, and the multi-mesh E2E runs in
a network namespace with loopback only (`scripts/netless.sh`).

### 2.1 Cohort election (PRD §11)

A cohort is one kind's members in one mesh. Every birth publishes, once, the instant it became ready
for traffic (`ready_since_ms` in its membership digest's `extra`). In each admin's view a cohort's
primary is its `ReadyForTraffic` member with the earliest claim, ties to the lowest ordinal; a member
without a claim ranks last. Every observer reads the same claims, so converged views agree:

- grow, restart and a recreated path claim a later instant and never displace the incumbent;
- when the primary leaves the view (killed, removed, retired), the next oldest succeeds it;
- a partition lets each side elect from what it hears; on heal the views agree again.

The fabric primary is the admin primary of the lowest-named mesh that has one; when that mesh is lost,
the next mesh's admin primary holds the fabric, and every admin's fabric and mesh views advertise the
live owning admin's control API. Each admin reports a change of a cohort's primary in its view as
`rafka.mesh.election.resolve.via-recompute`, and of the fabric primary as `…via-fabric-recompute`
(`crates/rafka-node-admin-core/src/election.rs`).

Which admin executes a Build operation (`crates/rafka-node-admin-core/src/executor.rs`,
`executor_for`; PRD §12.1): a mesh's admin primary runs the operations on that mesh's members; the
fabric primary runs mesh creation and retirement, every node-admin cohort, and a mesh's members while
that mesh has no admin primary. An admin claims a Build's next attempt only when it executes the first
operation left and no live admin holds an open attempt. An attempt that reaches an operation another
admin executes ends `handed-off`; that admin claims the next attempt of the same Build. A fabric Build
that creates a mesh therefore runs in two attempts: the fabric primary creates the mesh's admins, the
new mesh's primary creates its members. A Build can complete on an admin other than the one a client
asked; the client's admin view settles within gossip delay.

A lost mesh is recovered as itself (PRD §1.15, §12.2). The Build that was active when the mesh was
lost keeps its id: a surviving admin takes over its next attempt and re-creates the mesh's paths
under the mesh's known id. Each re-created node is a new birth: a new incarnation and endpoints the
allocator hands out now. A create step's receipt is reused only while the runtime it names still
runs, and a run that failed a step hands nothing on. Before a new birth takes a path, the previous
birth is fenced, unless it answers directly (Node RPC `Echo` for an rpc node, the control API for an
admin): a member gossip has not heard yet is not a dead one. Every launched node makes an entry pull to the admin that launched it,
over QUIC (`rafka_mesh_transport::entry`, ALPN `rafka-mesh-entry/1`): the answer is the fabric's policy
and the membership digests that admin hears, which the node records as heard before it is ready. A
launched admin's first view is therefore its launcher's, and it never plans from a view that is
missing live members. No node calls an admin's HTTP API; that API is for tests and people.

Control moves with the fabric primary. An admin that must retire, restart or stop a node another admin
launched adopts its runtime from the Build facts: the birth's `AllocateIdentity` receipt (incarnation,
deployment id) and the `DeployRuntime` receipt of that deployment (the handle). The fabric primary's
shutdown stops every live node of the fabric. A new birth at a path whose previous birth the view no
longer hears first stops that runtime if it still runs (`deployment.delete.via-fence`), so a path never
has two live runtimes.

Losing half a fabric costs a failure-detection window: until the dead peers' connections time out
(iroh's idle timeout), gossip may not deliver some survivors' digests, and they read as dead (~26 s
measured for MM losing mesh1).

## 3. Process contract (environment only)

| var | read by | meaning |
|---|---|---|
| `MESH_SPAWN_TYPE` | first `rafka-node-admin` | `process` or `container`; any other value refuses startup |
| `RAFKA_FABRIC` | `rafka-node-admin` (bootstrap) | fabric name, default `fabric1` |
| `RAFKA_MESH` | `rafka-node-admin` | the mesh this admin belongs to, default `mesh1` |
| `RAFKA_DATA_DIR` | every binary | node data dir: identity, journal, proof store |
| `RAFKA_NODE_ADMIN_API_BIND` | `rafka-node-admin` | HTTP bind, default `127.0.0.1:0` |
| `RAFKA_BIN_DIR` | `rafka-node-admin` | where `rafka-node-admin` / `rafka-rpc-node` live; default: beside the running exe |
| `RAFKA_EVIDENCE_DIR` | every binary | when set, every process writes its spans as JSONL here and passes the var to its children |
| `TRACEPARENT` | every spawned binary | W3C parent of the process's boot span: the deployment step that launched it |

A node-admin that is serving prints exactly one stdout line `RAFKA_NODE_ADMIN_API_BASE=<url>` and writes
`<data_dir>/node-admin.json` with the same `api_base`.

## 4. Control API (PRD §7)

Every topology mutation returns `202 {"build_id": "..."}` and does nothing else on the request path.

```text
POST   /api/build                 {"fabric": "fabric1", "meshes": [{"name": "mesh1", "node_admin": 2, "rpc_node": 3}]}
GET    /api/builds?id=<build_id>  {"build_id", "state": "pending|running|complete|failed", "intent", "attempt", "executor", "steps": [...]}
DELETE /api/builds?id=<build_id>  Build-history administration only
POST   /api/nodes/spawn           {"mesh": "mesh1", "kind": "rpc_node"}
DELETE /api/nodes/<path.name>
POST   /api/nodes/<path.name>/restart
GET    /api/nodes                 {"nodes": [NodeView]}
GET    /api/meshes/<id|name>      MeshView
GET    /api/fabric                FabricView
POST   /api/meshes                optional thin CreateMesh Build proxy
DELETE /api/meshes/<id|name>      thin RemoveMesh Build proxy
POST   /api/shutdown              runtime administration, not Build
```

`NodeView`:

```json
{
  "name": "mesh1.rpc.2", "kind": "rpc_node", "mesh": "mesh1",
  "node_id": "...", "fabric_id": "...", "incarnation_id": "...",
  "deployment_id": "...", "provider": "process", "data_dir": "...",
  "status": "pending|ready-for-traffic|draining|leaving|dead",
  "is_primary": false, "is_fabric_primary": false,
  "admin_api_base": null,
  "endpoints": [{"slot": "rpc-0", "addr": "127.0.0.1:41001", "freshness": "..."}]
}
```

`FabricView` and `MeshView` carry the live owning node-admin `admin_api_base` (PRD §1.16). Callers switch
control endpoints only from these views.

## 5. Probe (`rafka-rpc-probe`)

```text
rafka-rpc-probe --admin <api_base> <op> --target exact:<node_id>|path:<path.name> --key <u64>
               [--value <s>] [--expected <s>] [--pin <slot>=<freshness>] [--cut-before-finish]
op = put | get | delete | cas | echo
```

It prints one JSON line:

```json
{"outcome": "Reply|NotSent|Unserved|Indeterminate", "reason": "...",
 "reply": {"executing_node": "...", "executing_mesh": "...", "incarnation_id": "...",
           "slot": "rpc-0", "freshness": "...", "op": "put", "result": {...}}}
```

`--pin <slot>=<token>` asks the client to dial exactly that slot under that freshness token; a superseded
token is refused pre-commit (`NotSent`, reason `superseded`). `--cut-before-finish` writes part of the request
frame and then resets the stream (the 499 cut, PRD §1.18).

## 6. Evidence

With `RAFKA_EVIDENCE_DIR` set, each process appends finished spans to
`<dir>/<service>.<pid>.spans.jsonl`, one JSON object per line:

```json
{"trace_id": "...", "span_id": "...", "parent_span_id": "...", "name": "...",
 "service": "...", "start_unix_nano": 0, "end_unix_nano": 0, "attributes": {"build_id": "..."}}
```

Causality is asserted by `parent_span_id` only, never by timestamp enclosure (PRD §16). The
`TRACEPARENT` handed to a spawned process makes the process boot span a child of the deployment step that
launched it.

E2E artifacts land under `tests/artifacts/<feature>/<test>/` in the owning crate: `manifest.json`
(product/feature/subfeature/rung/provider/seed), `build-request.json`, `build-status.json`,
`nodes-before.json`, `nodes-after.json`, `rpc-ledger.jsonl`, `spans/` and `trace-url.txt`.

## 7. Span names (PRD §16)

Five segments, `rafka.<component>.<entity>.<action>.<reason>`, whitelist verbs only.

| span | where |
|---|---|
| `rafka.node_admin.build.create.via-rest` | a control route accepted a Build |
| `rafka.node_admin.build.reject.via-<reason>` | a Build refused by name (`via-provider-mismatch`, `via-invalid-intent`, ...) |
| `rafka.node_admin.build.update.via-reconcile` | one executor attempt reconciling desired − observed |
| `rafka.node_admin.node.create.via-build` / `node.update.via-build` / `node.delete.via-permanent-release` | per-node Build operation |
| `rafka.node_admin.deployment.update.via-pipeline` | one create/retire pipeline run (attrs `build_id`, `provider`) |
| `rafka.node_admin.deployment.update.via-step` | one pipeline step (attrs `step`, `build_id`, `provider`, `outcome`, `attempt`, `elapsed_ms`) |
| `rafka.mesh.node.create.via-deployment` | a spawned process's boot span (parent: the `DeployRuntime` step) |
| `rafka.mesh.election.resolve.via-recompute` / `via-fabric-recompute` | election outcomes |
| `rafka.node_rpc.request.serve.via-direct` | a dispatched invocation |
| `rafka.node_rpc.request.reject.via-unserved-tag` / `via-malformed` / `via-frame-not-sent` | refusals |
| `rafka.node_rpc.connection.evict.via-slot-superseded` / `via-incarnation-superseded` / `via-timeout-strikes` | the pool dropped a dial or connection (`crates/rafka-node-rpc/src/pool.rs`; pool identity `(scope, peer, incarnation, slot, freshness)`) |

New entities (`build`, `deployment`, `lifecycle_hook`) are recorded in `CLAUDE.md`'s span table in the
commit that first emits them.
