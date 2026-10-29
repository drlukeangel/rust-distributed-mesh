# Runbook: a stateful node's data dir across restart, re-birth and delete

A stateful node (Rafka's broker, a node-admin) keeps what must outlive its process in one place:
its data dir (`RDM_DATA_DIR`). This page answers one question per lifecycle operation: which data
dir the node runs on afterwards, which endpoint key it holds, and what its `pre_node_ready` hook is
told about it.

The contract is in these pages; this runbook only gathers them:
- [deployment-provider.md §4](../architecture/deployment-provider.md#4-storage-policy-and-the-data-dir), storage policy and the data dir;
- [node-restart.md](../architecture/lifecycles/node-restart.md) and [node-start.md](../architecture/lifecycles/node-start.md);
- [node-recovery.md](../architecture/lifecycles/node-recovery.md), with its data-dir rules (`R-FM2`);
- [node-replace.md](../architecture/lifecycles/node-replace.md) and [build-deployment.md](../architecture/build-deployment.md);
- [node-delete.md](../architecture/lifecycles/node-delete.md);
- [fabric-node-hooks.md](../architecture/fabric-node-hooks.md#what-the-context-carries), what `pre_node_ready` is given.

## The rules

- **Persistence belongs to the path.name, not to a birth.** Persistent storage survives stop,
  start and restart, and a new node created over an inherited data dir keeps it. Ephemeral storage
  is fresh for every new node (deployment-provider.md §4).
- **The path's config decides it, explicitly.** Every accepted Build carries each path's
  `PathConfig { storage, placement }`. Rafka's per-kind defaults are applied by Rafka's ingress
  adapter before acceptance: broker and node-admin Persistent, gateway and compute Ephemeral.
  Neither the pipeline nor the provider applies a hidden default.
- **RDM deletes nothing on disk, ever.** A delete releases the node.id and the name, never the data
  dir.
- **Only the data dir is carried.** The endpoint key (`node-key`) lives in it. Anything the
  application keeps outside `RDM_DATA_DIR` is not carried by RDM to a later birth. A broker's
  durable data must live under its `RDM_DATA_DIR`.

## By operation

| Operation | node.id, incarnation | Data dir | Endpoint key | `pre_node_ready` |
|---|---|---|---|---|
| `node.stop` then `node.start` | same | same | same | runs again, `start_kind = AfterStop` |
| `node.restart` | same; the process never exits | same | same, and the same bound port | runs again, `start_kind = AfterStop` |
| node recovery, verdict `exited` (a re-birth) | new; same path.name | the old one, inherited | the old one, loaded from it | `FirstStart`, `inherited = true` |
| node recovery, any other verdict but `os-stopped` | new; same path.name | newly allocated | newly minted | `FirstStart`, `inherited = false` |
| node recovery, the old data dir absent for `RDM_DATA_DIR_GONE_FLOORS` staleness floors | new; same path.name | newly allocated beside the old path; `data-dir-lost { path, since }` recorded | newly minted (the old one is never re-minted) | `FirstStart`, `inherited = false` |
| node recovery, the old data dir present but `node-key` unreadable | none yet | none: `data-dir-unreadable { path, cause }` recorded and the recovery held until the key reads | none | runs only once the key reads, `inherited = true` |
| executable replacement (node.replace), `Persistent` path | new; same path.name | the predecessor's, inherited | the predecessor's | `FirstStart`, `inherited = true` |
| executable replacement, `Ephemeral` path | new; same path.name | newly allocated | newly minted | `FirstStart`, `inherited = false` |
| `node.delete` | released | kept on disk, with its key; a later spec at the path may inherit it | kept | none |

A move of the data itself to another layout or place during a replacement is Rafka's own step,
with its own commit boundary (node-replace.md, "Data transfer").

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
| `rdm.node_admin.deployment.update.via-step` with `step=PrepareStorage` | the data dir prepared for a create: fresh, or inherited and inspected |
| `rdm.mesh.node.update.via-pre-node-ready-attempt`, field `inherited` | what the hook was told |
| `node.get` on the birth | `data-dir-lost` or `data-dir-unreadable` when either stands ([provider runbook](provider-runbook.md)) |
| the birth's row | a new node.id and incarnation at the same path.name after a re-birth; the same ones after a restart |

## Checking a broker restart

1. Confirm which operation ran. A `node.restart` keeps the node.id and incarnation. A re-birth at
   the same path.name shows new ones.
2. For a restart: the process never exited, and the data dir and port are unchanged. If the broker's
   data is missing afterwards, the broker's own stop and start released or rewrote it. RDM did not
   touch the disk.
3. For a re-birth: read `inherited` on the pre-node-ready attempt span.
   - `true`: the old data dir is in use, and the broker should open it.
   - `false`: read the recovery's verdict and any `data-dir-lost` fact. The broker started empty and
     must resync.
4. Check the path's storage in the accepted Build: a broker path should be `Persistent`.
