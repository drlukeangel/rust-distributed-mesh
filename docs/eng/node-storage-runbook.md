# Runbook: a stateful node's data dir across restart, re-birth and delete

A stateful node (Rafka's broker, a node-admin) keeps what must outlive its process in one place:
its data dir (`RDM_DATA_DIR`). This page answers one question per lifecycle operation: which data
dir the node runs on afterwards, which endpoint key it holds, and what its `pre_node_ready` hook is
told about it.

The contract is in these pages; this runbook only gathers them:
- [deployment-provider.md §4](../architecture/deployment-provider.md#4-storage-policy-and-the-data-dir), storage policy and the data dir;
- [node-stop.md](../architecture/lifecycles/node-stop.md) and [node-start.md](../architecture/lifecycles/node-start.md);
- [node-recovery.md](../architecture/lifecycles/node-recovery.md), with its data-dir rules (`R-FM2`);
- [node-name-update.md](../architecture/lifecycles/node-name-update.md) and [build-deployment.md](../architecture/build-deployment.md);
- [node-delete.md](../architecture/lifecycles/node-delete.md);
- [fabric-node-hooks.md](../architecture/fabric-node-hooks.md#what-the-context-carries), what `pre_node_ready` is given.

## The rules

- **Persistence belongs to the path and name, not to a birth.** Persistent storage survives stop,
  start and restart, and a new node created over an inherited data dir keeps it. Ephemeral storage
  is fresh for every new node (deployment-provider.md §4).
- **The path's config decides it, explicitly.** Every accepted Build carries each node's
  `storage_persistence`: the node's own, or its type's (`node-admin` persistent, `node` ephemeral, a
  declared type its own). Neither the pipeline nor the provider applies a hidden default.
- **RDM deletes nothing on disk, ever.** A delete releases the node.id and the name, never the data
  dir.
- **Only the data dir is carried.** RDM's own files for the node live in it too; a re-created node gets a new endpoint key written there, never its predecessor's (Q358). Anything the
  application keeps outside `RDM_DATA_DIR` is not carried by RDM to a later birth. A broker's
  durable data must live under its `RDM_DATA_DIR`.

## By operation

| Operation | node.id, incarnation | Data dir | Endpoint key | `pre_node_ready` |
|---|---|---|---|---|
| `node.stop` then `node.start` | same | same | same | runs again, `start_kind = AfterStop` |
| node recovery, the inspect's answer `exited`, `persistent` storage, the old data dir there | new; same path and name | the old one, inherited | newly minted, written into it | `FirstStart`, `inherited = true` |
| node recovery, `ephemeral` storage, the old data dir gone, or any other answer of the inspect | new; same path and name | newly allocated | newly minted | `FirstStart`, `inherited = false` |
| `node.delete` | released | kept on disk; a later node at the path may inherit it | never reused | none |

A new executable is a new node, a Build that adds it (build-deployment.md §3). To keep the old node's
data, the old node is stopped and renamed with node.name.update first; it keeps its data dir and key
until a Delete removes it, and moving its data is Rafka's own step, with its own commit boundary.

## What the application reads

- `NodeReadyCtx::inherited`, also in the node's environment as `RDM_INHERITED_DATA_DIR`
  (`1` or `0`). `true`: open what the data dir holds. `false`: the data dir is empty; resync from
  the other nodes.
- `start_kind` alone never says whether storage is inherited (fabric-node-hooks.md): a restart is
  `AfterStop` over the same data dir, and a re-birth is `FirstStart` over an inherited one.

## The container provider

- The data dir is a host directory, bind-mounted into the container at the same path
  (`-v <data dir>:<data dir>`). Removing the container leaves it on the host.
- The container runs as the launching user, so what the node writes stays that user's.

## What shows it

| Where | What it shows |
|---|---|
| `rdm.node_admin.deployment.update.via-step` with `step=PrepareStorage` | the data dir prepared for a create: fresh, or the predecessor's |
| `rdm.mesh.node.update.via-pre-node-ready-attempt`, field `inherited` | what the hook was told |
| the birth's row | a new node.id and incarnation at the same path and name after a re-birth; the same ones after a restart |

## Checking a broker restart

1. Confirm which operation ran. A `node.stop` followed by `node.start` keeps the node.id and incarnation. A re-birth at
   the same path and name shows new ones.
2. For a restart: the process never exited, and the data dir and port are unchanged. If the broker's
   data is missing afterwards, the broker's own stop and start released or rewrote it. RDM did not
   touch the disk.
3. For a re-birth: read `inherited` on the pre-node-ready attempt span.
   - `true`: the old data dir is in use, and the broker should open it.
   - `false`: read the inspect's answer on the recovery's span and the path's `storage_persistence`. The broker started empty and
     must resync.
4. Check the path's storage in the accepted Build: a broker path should be `Persistent`.
