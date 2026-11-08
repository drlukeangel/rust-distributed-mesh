# Storage

**Status:** ARCHITECTURE, the contract for the two things RDM calls storage.

Two things are called storage, and neither is the other:

| | what it holds | whose property | the question it answers |
|---|---|---|---|
| **`storage_persistence`** | a node's own data dir: its keys and secrets, and whatever the application keeps there, a broker's or gateway's WAL, a node-admin's themes and images | the node's, on its create spec and its record's `storage` | does this node's own storage survive its births, and what becomes of it once the node is gone |
| **`topology_storage_provider`** | RDM's five record stores: fabric, meshes, nodes, connections, builds | the fabric's, on the Fabric record | which backend RDM's records are written to |

## 1. `storage`: a node's own storage

A node's own storage is its data dir (`RDM_DATA_DIR`): `node-key`, `ca-secret`, `fabric-ca`,
`declared-secrets`, its own row, and the application's data. RDM reads and writes its own files
there and never reads the application's. `storage { persistence, data_dir }` is a field of the node record (Q364): `data_dir` is the node's `RDM_DATA_DIR`, set by its launch, never by a Build; `persistence` is set by the node's create spec, carried on the accepted Build's `NodeSpec`, and is three states:

| `storage_persistence` | across the node's births (stop, start, re-creation at the same `path` and `name`) | once the node is gone |
|---|---|---|
| `ephemeral` | fresh storage for every new node; nothing is inherited | nothing kept, so nothing to dispose of |
| `persistent { disposition: preserve }` | survives; a node created over the inherited data dir keeps it | kept |
| `persistent { disposition: release }` | survives, the same | may be removed by the application's own work; `node.delete` releases nothing |

The two columns are two questions. Ephemeral is about births: the node's storage does not survive
to the next one. The disposition is about storage that did survive, once the node is gone, and only
exists for storage that was kept. Ephemeral is therefore not a disposition, and the three states are
the three that mean something ([deployment-provider.md §4](deployment-provider.md): "the three-state
`StoragePersistence` contract is RDM law").

Rules:

- **Persistence belongs to the logical `path` and `name`, not its node.id.** A node re-created at the same `path` and `name` with `persistent` storage is launched on its predecessor's data dir when that directory exists; with `ephemeral` storage, or when it does not exist, on a new one. `RDM_INHERITED_DATA_DIR` tells the node which, surfaced to `pre_node_ready` as `inherited` ([deployment-provider.md §4](deployment-provider.md), [node-recovery.md](lifecycles/node-recovery.md)). Its endpoint is always new (Q358): RDM writes its own files for the new node there and never reads the predecessor's.
- **RDM deletes nothing on disk, ever.** `node.delete` releases no storage; `preserve` issues no
  release action; `release` is a statement to the application, which removes the storage by its own
  work or leaves it.
- **What is in the data dir is the application's to judge.** RDM does not look at the application's data; the node's `pre_node_ready` answers whether it is usable: `Ok`, `Pending` while it waits (for example a volume not yet mounted), `Failed` when it is not (Q350, Q359).
- **Defaults are the application's.** Rafka's per-kind defaults (broker and node-admin persistent,
  gateway and compute ephemeral) belong to its ingress adapter; normalization happens before Build
  acceptance, and an accepted Build carries the explicit value for every path.
- **`storage_persistence` changes only with a new node** ([node create](lifecycles/node-create.md)): a
  Build states it only for a node it creates.

A caller reads it as a field of the node's row, `node.get [storage_persistence: persistent]`.

## 2. `topology_storage_provider`: where RDM's records go

RDM's records have six storage boundaries: `fabric.storage`, `mesh.storage`, `nodes.storage`,
`connections.storage`, `builds.storage` and `events.storage` ([build.md §5.3](build.md#53-node-admin-storage-boundaries)).
They hold the fabric's topology and its history, what `fabric.get`, `mesh.get`, `node.get`,
`node.connection.get` and `build.get` read: every record whole, every write appended, nothing ever
removed, one writer per key ([node-identity-and-state.md §2.5](node-identity-and-state.md), R14). A
node never names where a record goes: it hands the record to the store's trait, and the backend
behind the trait knows where. Storage is persistence only; it stores and returns what it is given
and decides nothing (Q109). A property or lifecycle put retains the original backend operation
until it is terminal. Every returned outcome, including an error, means that issued mutation can
no longer commit later; an error may still conceal a committed value whose acknowledgement was
lost. Caller cancellation or a transport deadline does not release serialization or show absence.

A **provider** is a backend supplying all six storage boundaries, compiled into an executable and
registered in its wiring by name:

| provider | registered by | where the records go |
|---|---|---|
| `file` | every RDM binary | one append-only JSONL file per store under the fabric's shared store root (`RDM_STORE_DIR`), `fabric.jsonl`, `meshes.jsonl`, `nodes.jsonl`, `connections.jsonl`, `builds.jsonl`, `events.jsonl`, with the `layout` marker; every line a CloudEvents 1.0 event, the whole record its `data`, its key its `subject`, its writer its `source`, the write its `id` (a time-ordered UUIDv7) and `time`, its kind and schema version its `type` and `dataschema`; the latest line for a `subject` its current state, nothing removed |
| the application's | the application's binaries, at `start_on` | its own stores; Rafka binds the traits to its system topics |

A provider is one set of stores for the fabric, reached by every node that writes: a node's own record is written by the node and read by its primaries. A backend that keeps its stores in one process, the testkit's `memory` backend, serves a single-process fabric only.

A name is bound to its place at registration. A node can move only between the providers its own
executable registers; a provider no executable has needs a new node on a new executable (a Build that adds it), not a
storage change.

**The property.** `topology_storage_provider` is one value for the fabric, on the Fabric row, beside
`previous_topology_storage_providers`, each provider the fabric left, in order. The same name on a
node's row is the provider that node writes, recorded by RDM and never put by a caller: the fabric's
value is the intent, the node's is the fact, and during a change they differ, which is the progress.

| | who sets it | from |
|---|---|---|
| Day 0 | the bootstrap admin, on the Fabric row | its environment, `RDM_TOPOLOGY_STORAGE_PROVIDER`, `file` when unset |
| every launch | the launching admin, into the node's environment | the Fabric row and any change in progress |
| a restart with no launcher (the adoption launch) | the node itself | its own row in its data dir |
| a change | the application, by `fabric.storage.update { topology_storage_provider }` to the fabric-primary, once its store exists | the change's events |
| the end of a change | the fabric-primary, on the Fabric row | the commit |

### Changing it

A storage change is the fabric-primary's own workflow, never a Build: it adds and removes no node
([build.md §1](build.md#1-build-is-the-only-topology-mutation-engine), Q342). The application requests
it with `fabric.storage.update { topology_storage_provider }`, a single call to the fabric-primary as
[`fabric.root.update`](lifecycles/fabric-ca-renew.md#fabricrootupdate-the-caller-op) is. One change runs
at a time and never beside a Build in progress: each refuses the other `build-in-progress` by name. The
fabric, every mesh and every node stay ready, no node restarts, and no node changes
state. Acceptance refuses by name a provider the fabric-primary's wiring does not register or cannot
open. The change has a durable `change_id`, recorded as its `rdm.storage.started` event before its first command; its
commands carry it, and a node refuses by name a command of an older one (`rejected-stale-change`).
It is four steps:

1. **Block.** The fabric-primary sends `mesh.block` to every mesh-primary. Each mesh-primary waits
   until its in-flight commands complete and every required node, including a stopped node, has acknowledged its freeze. An unanswered freeze is resolved through
   its spent call ([Node RPC §43](node-rpc.md#43-send-delivery-is-the-runtimes); Q332, Q344) and node investigation/recovery before `blocked`; no
   second timer or unresolved-investigation state is added. It then replies `blocked` and
   gossips the current topology of its mesh. A mesh-primary that has replied `blocked` issues no new command until it is unblocked.
2. **Copy.** Once every mesh-primary has replied `blocked`, the fabric-primary appends every line of
   all six stores on the old provider to the new one in source order, including both node-record
   kinds, immutable launch copies and unresolved property obligations. A copy resumed after a lost answer
   appends nothing twice; a new provider holding lines the old one does not is refused by name, and
   nothing switches.
3. **Switch.** The fabric-primary appends the change's `rdm.storage.committed` event for
   its `change_id` to the new provider's events store, then appends the Fabric row's line naming the new
   provider current and the old in `previous_topology_storage_providers`. Whoever resumes the change
   learns the decision only from that record.
4. **Unblock.** The fabric-primary sends `mesh.unblock` to every mesh-primary. Each mesh-primary moves
   its nodes to the new provider, then replies `unblocked` and issues commands again.
   A mesh-primary told `mesh.unblock` reads the change's `rdm.storage.committed` event on the new
   provider first (an events-store read by name, Q292); present, it moves its own stores there;
   absent, the change is not committed and it stays where it is. A read error fails the unblock by
   name, never as absence.
   A node that refuses its move is re-created on the new provider by its mesh-primary's node
   recovery, never part of the change, before its mesh-primary replies `unblocked`.

A mesh-primary that does not reply `blocked` or `unblocked` is a lost node, as any node is
([node investigation](lifecycles/node-investigation.md)): it is investigated, and on any verdict but
`reached` node recovery re-creates it, the primary's own workflow, never part of the change. After
the switch the change completes, its `rdm.storage.ended` event written, once every mesh-primary has
replied `unblocked`; nothing else waits.

No Build and no other change is admitted while a change is in progress; `fabric.delete` alone is
admitted, and it interrupts the change as it interrupts any Build,
the records on both providers kept. A step that fails before the switch fails the change only once the
fabric-primary has read that no `rdm.storage.committed` event exists for its `change_id`; the fabric-primary then
sends `mesh.unblock` on the old provider, the new provider closed and the Fabric row unchanged. No timer ends a block. The previous provider is kept as it was at the switch, named on
the Fabric row, written no more and never deleted by RDM. A later change naming it again copies into
it only the lines it lacks.

A caller reads the fabric's with `fabric.get` and each node's with
`node.get [topology_storage_provider: file]`.

## 3. What is not storage

- A node's status and the fabric's states are not stored by their holders: they are recorded on the
  rows by their writers and disseminated by gossip, which is never persisted and never the source of
  truth ([gossip.md §8](gossip.md#8-entity-projection-gossip-remains-projection-not-source-of-truth)).
- The application's data plane, a broker's WAL or a gateway's, is the application's: it lives under
  the node's `storage_persistence` and RDM never reads it.
- A node's `storage.data_dir` on its record names where its own storage is; it is a fact about the node,
  not a provider.

## Environment

`RDM_DATA_DIR`, `RDM_INHERITED_DATA_DIR`, `RDM_STORE_DIR`,
`RDM_TOPOLOGY_STORAGE_PROVIDER` (the table in `CLAUDE.md`).
