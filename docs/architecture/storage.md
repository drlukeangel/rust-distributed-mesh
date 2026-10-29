# Storage

**Status:** ARCHITECTURE, the contract for the two things RDM calls storage.

Two things are called storage, and neither is the other:

| | what it holds | whose property | the question it answers |
|---|---|---|---|
| **`storage_persistence`** | a node's own data dir: its keys and secrets, and whatever the application keeps there, a broker's or gateway's WAL, a node-admin's themes and images | the node's, on its create spec and its row | does this node's own storage survive its births, and what becomes of it once the node is gone |
| **`topology_storage_provider`** | RDM's five record stores: fabric, meshes, nodes, connections, builds | the fabric's, on the Fabric row; the node's row carries the provider the node writes | which backend RDM's records are written to |

## 1. `storage_persistence`: a node's own storage

A node's own storage is its data dir (`RDM_DATA_DIR`): `node-key`, `ca-secret`, `fabric-ca`,
`declared-secrets`, its own row, and the application's data. RDM reads and writes its own files
there and never reads the application's. `storage_persistence` is a typed property of the node's
create spec, carried on the accepted Build's `NodeSpec` and the node's row, and is three states:

| `storage_persistence` | across the node's births (stop, start, re-creation at the same `path.name`) | once the node is gone |
|---|---|---|
| `ephemeral` | fresh storage for every new node; nothing is inherited | nothing kept, so nothing to dispose of |
| `persistent { disposition: preserve }` | survives; a node created over the inherited data dir keeps it | kept |
| `persistent { disposition: release }` | survives, the same | may be removed by the application's own work; `node.delete` releases nothing |

The two columns are two questions. Ephemeral is about births: the node's storage does not survive
to the next one. The disposition is about storage that did survive, once the node is gone, and only
exists for storage that was kept. Ephemeral is therefore not a disposition, and the three states are
the three that mean something ([deployment-provider.md §4](deployment-provider.md): "the three-state
`StorageMeta` contract is RDM law").

Rules:

- **Persistence belongs to the logical `path.name`, not its incarnation.** A node recovered or
  replaced at the same `path.name` with `persistent` storage inherits the predecessor's data dir and
  endpoint key; with `ephemeral` it is given a fresh one. `RDM_INHERITED_DATA_DIR` tells the birth
  which, surfaced to `before_ready` as `inherited` ([deployment-provider.md §4](deployment-provider.md),
  [node-recovery.md](lifecycles/node-recovery.md), [node-replace.md](lifecycles/node-replace.md)).
- **RDM deletes nothing on disk, ever.** `node.delete` releases no storage; `preserve` issues no
  release action; `release` is a statement to the application, which removes the storage by its own
  work or leaves it.
- **A lost or broken data dir is told apart by the error, never guessed.** Gone, the directory absent
  past `RDM_DATA_DIR_GONE_FLOORS` staleness floors, is recovered at the same `path.name` over a fresh
  data dir and recorded `data-dir-lost`; broken, the directory present with `node-key` missing or
  unreadable, holds the recovery and is recorded `data-dir-unreadable` (R-FM2,
  [deployment-provider.md §4](deployment-provider.md)).
- **Defaults are the application's.** Rafka's per-kind defaults (broker and node-admin persistent,
  gateway and compute ephemeral) belong to its ingress adapter; normalization happens before Build
  acceptance, and an accepted Build carries the explicit value for every path.
- **A change of `storage_persistence` alone is a property Build**: the fabric stays ready
  ([states.md](lifecycles/states.md), "Resize and executable replacement").

A caller reads it as a field of the node's row, `node.get [storage_persistence: persistent]`.

## 2. `topology_storage_provider`: where RDM's records go

RDM's records are five stores, one per entity: `fabric.storage`, `mesh.storage`, `nodes.storage`,
`connections.storage` and `builds.storage` ([build.md §5.3](build.md#53-node-admin-storage-boundaries)).
They hold the fabric's topology and its history, what `fabric.get`, `mesh.get`, `node.get`,
`node.connection.get` and `build.get` read: every record whole, every write appended, nothing ever
removed, one writer per key ([node-identity-and-state.md §2.5](node-identity-and-state.md), R14). A
node never names where a record goes: it hands the record to the store's trait, and the backend
behind the trait knows where. Storage is persistence only; it stores and returns what it is given
and decides nothing (Q109).

A **provider** is a backend implementing those five traits, compiled into an executable and
registered in its wiring by name:

| provider | registered by | where the records go |
|---|---|---|
| `file` | every RDM binary | one append-only JSONL file per store under the fabric's shared store root (`RDM_STORE_DIR`), `fabric.jsonl`, `meshes.jsonl`, `nodes.jsonl`, `connections.jsonl`, `builds.jsonl`, with the `layout` marker and the `locks/` of the name claims and in-flight rows; every line a whole record with its writer's operation identity, the latest line for a key its current state, nothing removed |
| the application's | the application's binaries, at `start_on` | its own stores; Rafka binds the traits to its system topics |

A name is bound to its place at registration. A node can move only between the providers its own
executable registers; a provider no executable has is an executable replacement, not a storage
change.

**The property.** `topology_storage_provider` is one value for the fabric, on the Fabric row, beside
`previous_topology_storage_providers`, each provider the fabric left, in order. The same name on a
node's row is the provider that node writes, recorded by RDM and never put by a caller: the fabric's
value is the intent, the node's is the fact, and during a change they differ, which is the progress.

| | who sets it | from |
|---|---|---|
| Day 0 | the bootstrap admin, on the Fabric row | its environment, `RDM_TOPOLOGY_STORAGE_PROVIDER`, `file` when unset |
| every launch | the launching admin, into the node's environment | the Fabric row and any Build in progress |
| a restart with no launcher (the adoption launch) | the node itself | its own row in its data dir |
| a change | the application, by `build.create` naming the new value, once its store exists | the Build; a Build that omits the property leaves it unchanged (Q166) |
| the end of a change | the fabric-primary, on the Fabric row | the commit |

### Changing it

A storage change is a Build that carries the complete topology with the same meshes and the new
`topology_storage_provider` ([build.md §1](build.md#1-build-is-the-only-topology-mutation-engine)). It
is admitted and refused as any Build is, one in progress at a time, and it is a property Build: the
fabric, every mesh and every node stay ready, no node restarts, and no node changes
state. Acceptance refuses by name a provider the fabric-primary's wiring does not register or cannot
open. The change has a durable `change_id`, recorded with its Build before its first command; its
commands carry it, and a node refuses by name a command of an older one (`rejected-stale-change`).
It is four steps:

1. **Block.** The fabric-primary sends `mesh.block` to every mesh-primary. Each mesh-primary waits
   until its in-flight commands complete, then replies `blocked` and gossips the current topology of
   its mesh. A mesh-primary that has replied `blocked` issues no new command until it is unblocked.
2. **Copy.** Once every mesh-primary has replied `blocked`, the fabric-primary appends every line of
   every store on the old provider to the new one in source order, every name claim and in-flight row
   with its exact claimant. A copy resumed after a lost answer appends nothing twice; a new provider
   holding lines the old one does not is refused by name, and nothing switches.
3. **Switch.** The fabric-primary inserts `commit` for `(build_id, change_id)` on the new provider
   with the store's insert-and-fail primitive, then appends the Fabric row's line naming the new
   provider current and the old in `previous_topology_storage_providers`. Whoever resumes the change
   learns the decision only from that record.
4. **Unblock.** The fabric-primary sends `mesh.unblock` to every mesh-primary. Each mesh-primary moves
   its nodes to the new provider, then replies `unblocked` and issues commands again.
   A node that refuses its move is re-created at its `path.name` on the new provider, an attempt of
   the change's own Build, before its mesh-primary replies `unblocked`.

A mesh-primary that does not reply `blocked` or `unblocked` is a lost node, as any node is
([node investigation](lifecycles/node-investigation.md)): it is investigated, and on any verdict but
`reached` node recovery re-creates it at its `path.name`, an attempt of the change's own Build. After
the switch the Build completes once every mesh-primary has replied `unblocked`; nothing else waits.

No Build is admitted while a change is in progress, a change of `topology_storage_provider`
included; `fabric.delete` alone is admitted, and it interrupts the change as it interrupts any Build,
the records on both providers kept. A step that fails before the switch fails the Build only once the
fabric-primary has read that no `commit` exists for `(build_id, change_id)`; the fabric-primary then
sends `mesh.unblock` on the old provider, the new provider closed and the Fabric row unchanged. No timer ends a block. The previous provider is kept as it was at the switch, named on
the Fabric row, written no more and never deleted by RDM. A later Build naming it again copies into
it only the lines it lacks.

A caller reads the fabric's with `fabric.get` and each node's with
`node.get [topology_storage_provider: file]`.

## 3. What is not storage

- A node's status and the fabric's states are not stored by their holders: they are recorded on the
  rows by their writers and disseminated by gossip, which is never persisted and never the source of
  truth ([gossip.md §8](gossip.md#8-entity-projection-gossip-remains-projection-not-source-of-truth)).
- The application's data plane, a broker's WAL or a gateway's, is the application's: it lives under
  the node's `storage_persistence` and RDM never reads it.
- A node's `data_dir` field on its row names where its own storage is; it is a fact about the node,
  not a provider.

## Environment

`RDM_DATA_DIR`, `RDM_INHERITED_DATA_DIR`, `RDM_DATA_DIR_GONE_FLOORS`, `RDM_STORE_DIR`,
`RDM_TOPOLOGY_STORAGE_PROVIDER` (the table in `CLAUDE.md`).
