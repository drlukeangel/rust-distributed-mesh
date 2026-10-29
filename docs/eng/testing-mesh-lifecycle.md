# A test's fabric: bring-up, restart, teardown

RDM has no standing fabric for a lane. Every e2e cell births its own **estate**: the first
node-admin of a new fabric, started by the cell, and every node that node-admin's Builds launch
(`crates/rdm-test/src/estate.rs`). Nothing is kept between cells and nothing is shared between
lanes: two cells never read each other's fabric, store or spans. This page is that estate's life,
from its root to its teardown. Driving it is [testing-with-node-admin.md](testing-with-node-admin.md);
the harness calls are [testing-harnesses.md](testing-harnesses.md).

## 1. The estate's folders

| folder | what it holds | source |
|---|---|---|
| the estate root, `<temp dir>/i143-<test>-<pid>-<n>` | one per estate: a cell that ends one estate and births another never shares a root | `estate.rs::estate_root` |
| `<root>/store` | the fabric's one store root, handed to every node-admin the estate starts and, through each launch, to every node of the fabric | `estate.rs::estate_store_dir` |
| `<root>/<mesh>.admin.1` | the Day-0 admin's data dir | `Estate::born` |
| the artifacts, `<RDM_ARTIFACTS_DIR or crates/rdm-test/tests/artifacts>/<feature>/<test>/` | the estate's `manifest.json`, the RPC ledger, `trace-url.txt` and the cell's own artifacts; removed and made again at each birth | `estate.rs::artifacts_root`, `Estate::born` |
| `<artifacts>/spans/` | the evidence directory: every process's `*.spans.jsonl` | `Estate::born` (`RDM_EVIDENCE_DIR`) |

The manifest records the owner (product, feature, subfeature, rung, provider, test), the seed
when the run has one, the entry admin and the executable bindings (`Estate::write_manifest`).

## 2. Bring-up

`Estate::bootstrap(owner, fabric, mesh)` (or a variant, [testing-harnesses.md](testing-harnesses.md) §3):

1. makes the root and its store, and starts the root's reaper (`EstateScope::begin`, §5);
2. starts `rdm-node-admin` from the test executable's own build (or the explicit binding set) with
   `MESH_SPAWN_TYPE`, `RDM_FABRIC`, `RDM_MESH`, `RDM_DATA_DIR`, `RDM_STORE_DIR`, `RDM_BIN_DIR`,
   `RDM_EVIDENCE_DIR`, `RDM_ESTATE_ROOT` and the test kit's `RDM_CA_SECRET`: the Day-0 admin of a new
   fabric ([fabric-create.md](../architecture/lifecycles/fabric-create.md));
3. waits up to 30 s for the admin to print its birth (`RDM_NODE_BIRTH=`) and its control base
   (`RDM_NODE_ADMIN_API_BASE=`);
4. waits until `fabrics.get` at that admin names the fabric-primary, the Fabric's id and the current
   Build, the Day-0 Build it accepted at its start;
5. follows that Day-0 Build's stream to its `Complete`, so a cell's first `build.create` is never
   refused for a Build in progress it did not start. A `Failed` frame, or the fabric-primary moving
   while it runs, fails the bring-up by name.

The cell then grows the fabric with a Build (`estate.submit`, [testing-with-node-admin.md](testing-with-node-admin.md)
§3) and waits for `until_fabric_ready` before any fault.

## 3. The provider

`MESH_SPAWN_TYPE` (`process` or `container`) is read once by the first node-admin and fixed as
fabric policy; a Build never names a provider (CLAUDE.md env table). The estate's owner names it:

- **process**: every node is a host process; its data dir holds `deployment.json` with its pid.
- **container**: the harness mints the Fabric id first, so the fabric's bridge network
  (`rafka-<fabric id>`) exists before its first admin; the Day-0 admin runs in a container on that
  network and drives the host's Docker daemon through its socket. Every container of the fabric is
  labelled `rafka.fabric=<fabric id>` (`estate.rs::spawn_admin_in_container`, `REAPER`). The
  subnet comes from `RDM_CONTAINER_SUBNET_POOL`. A host without Docker fails a container cell by
  name under `RDM_REQUIRE_CONTAINER=1`.

## 4. Restarting in place

| what happens | what is kept | page |
|---|---|---|
| `node.stop` then `node.start` | the same process, node id and incarnation; readiness only after every start gate passes again | [node-stop.md](../architecture/lifecycles/node-stop.md), [node-start.md](../architecture/lifecycles/node-start.md) |
| a node-admin started again on its data dir (`estate.restart_admin(data_dir)`) | its identity, read back from the data dir | [node-admin-lifecycle.md](../architecture/node-admin-lifecycle.md) |
| a killed node, recovered by its mesh-primary | the same `path.name`, re-born with a new node id | [node-recovery.md](../architecture/lifecycles/node-recovery.md) |
| a node deleted | nothing: delete releases its identity and name; its storage stays | [node-delete.md](../architecture/lifecycles/node-delete.md) |

`node.restart` is removed (Q183): a node is stopped then started, or re-born (CLAUDE.md
"Vocabulary"). A node's ports are bound at port 0 at every birth and reported at its join, so a
re-born node's address is read from its row, never remembered from before
(CLAUDE.md, `RDM_TRANSPORT_ADDR`, `RDM_LISTENERS`).

`node_recovery_due()` in `rdm-test`'s test executable (`crates/rdm-test/tests/main.rs`) is the bound a cell waits for a
killed node's recovery: one staleness floor of silence, the reach ladder, the inspection and the
recovery Build's run, computed from the cadence in force.

## 5. Teardown

- `estate.stop().await` stops the whole estate through its own provider and waits until every
  runtime of it has exited: every admin the estate started, every runtime an admin launched and
  every container of its Fabric is sent SIGTERM at once, and each leaves as a local stop does
  (`rdm.mesh.node.delete.via-signal`), its evidence flushed. A runtime still running past the
  bound is named and killed; the pids so killed are returned. This is the test's teardown, not a
  fabric operation (`estate.rs`, `Estate::stop`).
- `estate.shutdown().await` is `stop`, consuming the estate.
- **Dropping an estate** (a failed run, a panic) sends every process of it SIGTERM, so its evidence
  flushes, waits up to 20 s, then kills whatever still runs under the root until nothing is alive
  (`impl Drop for Estate`).
- **The reaper.** `EstateScope::begin` starts a shell in its own session that waits for the test
  binary to die, however it dies, then kills every process whose environment names the root
  (`RDM_ESTATE_ROOT`, or an `RDM_DATA_DIR` under it), removes the containers labelled with the
  fabric recorded in `<root>/fabric_id` and its network, and removes the root. A gate that kills a
  job's process group leaves the reaper running.
- **The gate checks it.** After all jobs end, a node process of the lane's build that `systemd` or
  `init` has adopted is `FAIL leaked-processes` (`scripts/rdm-gate-at.sh`).
- Deleting a fabric as an operation is [fabric.delete](../architecture/lifecycles/fabric-delete.md),
  mesh by mesh, the fabric-primary's own mesh last; a cell that proves it submits it like any other
  operation.

## 6. Checking an estate before blaming a cell

1. Read the job's own log in the gate's `<log>.d/<job>.log`, or the cell's output: a bounded wait
   names what it last saw.
2. Read the estate's spans (`<artifacts>/spans/*.spans.jsonl`): every process wrote its own file.
   Build the timeline from them, every create, delete and terminate across all processes.
3. Read a node's data dir: `deployment.json` (its pid, process provider) and `transport-diag.log`
   (its handshake and path lines, written only in an evidence run;
   `crates/rdm-transport/src/transport_diag.rs`).
4. After a red, the gate keeps one snapshot of the scenario artifacts under
   `$HOME/rust-distributed-mesh-deploy/kept/` (`scripts/keep-gate-evidence.py`).

A cause is stated with its span lines; without them it is unattributed.

## 7. A live estate by hand: `scripts/demo-estate.sh`

For a person, not a cell: the R-shape consumer's estate, live, with the demo admin UI
(`scripts/demo-estate.sh`, header):

```bash
RDM_DEMO_BIN=<consumer bin dir> scripts/demo-estate.sh up     [<root>]   # Day-0 admin, the UI, two meshes by one Build
RDM_DEMO_BIN=<consumer bin dir> scripts/demo-estate.sh stream [<root>]   # five demo/echo streams through rdm-rpc-probe
RDM_DEMO_BIN=<consumer bin dir> scripts/demo-estate.sh down   [<root>]   # fabric.delete, then the fabric-primary's node.delete
```

Every `up` names a new fabric and new meshes (`fabric-<tag>`, `mesh-<tag>-1`, `mesh-<tag>-2`), so no
run reuses another's names. Every process exports to `OTEL_EXPORTER_OTLP_ENDPOINT` (default the
`.99` collector), and the UI reads flames from `JAEGER_QUERY_URL` (default Jaeger on `.99`).
`RDM_DEMO_BIN` holds the consumer's `rshape-node-admin`, `rshape-gateway`, `rshape-compute`,
`rshape-broker` and `rdm-admin-ui`, built from `demo/`.
