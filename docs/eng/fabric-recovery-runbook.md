# Runbook: launching a node-admin at the lost fabric-primary's path.name

When no node-admin of the fabric is alive, nothing inside the fabric brings one back. This happens to a
one-mesh fabric (Rafka's SN shape: one node-admin, `mesh1.admin.1`, which is the fabric-primary and
mesh1's primary) the moment that node-admin dies, even while the mesh's other nodes keep serving. A
person or a script launches one `rdm-node-admin` at the lost fabric-primary's path.name: the adoption
launch. The launch takes back the fabric-primary role and the mesh-primary role, and the fabric heals
the rest by itself.

This page covers:
- what to set;
- what the launch does on its own afterwards;
- which spans show each step;
- what refuses the launch.

The contract is in these pages; this runbook only reads them for the person on call:
- [fabric-recovery.md "Nothing alive"](../architecture/lifecycles/fabric-recovery.md#nothing-alive), Q156;
- [node-admin-lifecycle.md §4.4](../architecture/node-admin-lifecycle.md#44-fabric-primary-recovery-and-who-starts-it);
- [node-recovery.md](../architecture/lifecycles/node-recovery.md) for the mesh's other nodes.

The end-to-end cell is
`elections::fabric_takeover::the_sole_admin_lost_with_its_members_serving_is_adopted_by_a_launch_at_its_path_name_that_becomes_fabric_primary`
(`crates/rdm-test/tests/elections/fabric_takeover.rs`).

## Before you start

- **Confirm that no node-admin is alive.** While any node-admin lives, the fabric moves the
  fabric-primary by itself ([the loss of the fabric-primary](../architecture/lifecycles/fabric-recovery.md#the-loss-of-the-fabric-primary-while-a-node-admin-lives)),
  and an adoption launch is refused by name (Q159).
- **Find the fabric id.** It is the name of the fabric's directory under the store root
  (`<RDM_STORE_DIR>/<fabric id>`), and the bootstrap admin's `RDM_FABRIC_ID`.
- **Find the lost fabric-primary's path.name.** In a one-mesh fabric it is `mesh1.admin.1`. The store's
  node rows record it; a launch at any other path.name is refused by name.

## The launch

Start the consumer's node-admin executable (the one the Day-0 admin ran), with no launcher:

| Variable | Value |
|---|---|
| `RDM_NODE_NAME` | the lost fabric-primary's path.name, e.g. `mesh1.admin.1` |
| `RDM_FABRIC_ID` | the fabric's id |
| `RDM_MESH` | that path.name's mesh, e.g. `mesh1` |
| `RDM_STORE_DIR` | the fabric's store root, the same one the Day-0 admin was given |
| `RDM_DATA_DIR` | the lost fabric-primary's data dir when it is still on disk; otherwise a new directory |
| the fabric root and CA | read from an inherited `RDM_DATA_DIR` (`ca-secret`, `fabric-ca`); with a new data dir, set `RDM_CA_SECRET`, `RDM_FABRIC_CA_KEY`, `RDM_ISSUING_KEY` and `RDM_ISSUING_CERT` (the cell's `test_certs::custody_of`) |
| the declared values (Q150) | read back from an inherited data dir (`declared-secrets`); with a new data dir, each declared name in this process's environment, as the Day-0 admin had them |
| `MESH_SPAWN_TYPE` | unset, or the fabric's own provider: the provider is the Fabric record's, and a differing value is refused by name |
| `RDM_EXECUTABLE_DIRS` | as the Day-0 admin had it (required under the container provider) |
| `OTEL_EXPORTER_OTLP_ENDPOINT`, `RDM_EVIDENCE_DIR`, `RUST_LOG`, `RUST_BACKTRACE` | as for the Day-0 admin; each is passed on to every node it launches |

Do not set:
- `RDM_NODE_ID`, `RDM_INCARNATION_ID` or `RDM_LAUNCHER`: an adoption launch has no launcher.
- `RDM_MESH_PRIMARY` or `RDM_FABRIC_PRIMARY`: the role comes from the recorded primaries and the
  restore, never from a flag, and either flag is refused by name.
- `RDM_SEEDS`: the mesh's nodes are read from the store.

Once it serves, it prints `RDM_NODE_ADMIN_API_BASE=rdm://<node id>`, with a new node id. Enter the
fabric there.

## What it does on its own

Nothing more is commanded. In order:

1. **Reads the store with nothing written.** The store root must hold the Fabric record with its
   provider, the mesh row, a fabric-primary at `RDM_NODE_NAME` and a current Build. It mints no fabric
   id and no mesh id.
2. **Claims the fabric-recovery attempt** of the current Build for the mesh, before any other write.
   The claim is the mesh's in-flight row on the store root. A second launch's claim loses, and that
   launch exits.
3. **Installs the recorded primaries** (Q149) and runs node investigation on the lost birth. If the
   lost birth is reached, or any node-admin answers, the launch is refused `NameHeld` and exits, and the
   attempt closes through the failure path.
4. **Releases the lost birth's name** and re-creates the birth as itself: same path.name, new node.id
   and incarnation, and its own member cert as its own maker.
5. **Joins the live nodes** and adopts rafka-time from them. With no other node alive, its own clock is
   rafka-time.
6. **Becomes fabric-primary and mesh-primary.** Once it is Ready, step 4 of fabric
   recovery restores it to both roles, and the attempt completes.
7. **Heals the mesh by itself**, as mesh-primary:
   - A node that kept serving keeps its birth (node.id and incarnation) and stays
     ready. It is never re-created.
   - A node that is gone is recovered through node recovery, never waited for: node.delete of the
     exited birth, then node.create at the same path.name with a new node.id, over the inherited
     data dir when the provider found the runtime exited.
   - An old process on an inherited data dir that comes back is refused at admission, and its
     runtime is terminated.
   - Every later Build runs as before. The cell grows mesh1 by a Build after the adoption.

## What shows it in Jaeger

| Span | What it shows |
|---|---|
| `rdm.node_admin.fabric.update.via-adoption-claim` | the claim; `outcome=won` and `lost_node_id` = the lost birth |
| `rdm.node_admin.node.delete.via-node-row` with the lost `node_id` | the lost birth's name released, after the claim |
| `rdm.mesh.entry.update.via-rafka-time-adopted` with `source=pull` | rafka-time adopted from a live node |
| `rdm.node_rpc.status.update.via-declare-own` from the mesh's other nodes | each serving node stays ready |

`node.get` on the new node-admin then shows `is_fabric_primary` and `is_primary`.

## What refuses the launch

Each refusal is a line on stderr naming the input; nothing is written to the store.

| Cause | Fix |
|---|---|
| the store root holds no Fabric record of `RDM_FABRIC_ID` | the wrong store root or fabric id |
| `RDM_MESH` differs from `RDM_NODE_NAME`'s mesh, or the path.name is not a node-admin's | name the lost fabric-primary |
| `MESH_SPAWN_TYPE` differs from the Fabric record's provider | unset it |
| no root (`RDM_CA_SECRET` or a `ca-secret` in the data dir) | hand the custody, or point `RDM_DATA_DIR` at the lost fabric-primary's data dir |
| `RDM_DATA_DIR` is another birth's data dir | an inherited data dir must be the lost birth's own |
| a node-admin is alive (`NameHeld`, Q159) | the fabric moves the fabric-primary itself; no launch is needed |

If the launch passes all of these, its claim and step 4 make it the fabric-primary. A second launch
started by mistake loses the claim and exits.
