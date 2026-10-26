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

| `storage_persistence` | across the node's births (stop, start, restart, re-creation at the same `path.name`) | once the node is gone |
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
- **A change of `storage_persistence` alone is a property Build**: the fabric stays ready-for-traffic
  ([states.md](lifecycles/states.md), "Resize and executable replacement").

A caller reads it as a field of the node's row, `nodes.get [storage_persistence: persistent]`.

## 2. `topology_storage_provider`: where RDM's records go

RDM's records are five stores, one per entity: `fabric.storage`, `mesh.storage`, `nodes.storage`,
`connections.storage` and `builds.storage` ([build.md §5.3](build.md#53-node-admin-storage-boundaries)).
They hold the fabric's topology and its history, what `fabrics.get`, `meshes.get`, `nodes.get`,
`connections.get` and `builds.get` read: every record whole, every write appended, nothing ever
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
fabric, every mesh and every node stay ready-for-traffic, no node restarts, and no node changes
state. The progress is the Build's step receipts and each node's row. Acceptance refuses by name a
provider the fabric-primary's wiring does not register or cannot open. The change is three steps,
each fabric-wide, carried fabric-primary → mesh-primary → node as the Build's ordinary order:

Each migration run has a durable `change_id`, recorded with its Build before any migration command.
Commands, freeze acknowledgements, copy progress and the decision carry that identity. Recovery of
an unfinished run reuses it, including when its decision is uncertain. After a confirmed abort and
completion of the rollback, resubmitting the same Build starts a new run with a new `change_id`.
Nodes retain the resolved run identity and refuse delayed commands from an older run by name;
accepting a new run does not erase that protection. A committed run only resumes release.

1. **Open.** Every node opens the new provider beside the old, writes nothing to it, and reports. A
   node whose executable does not register the provider fails the step by name, naming the node and
   the provider, and the Build fails with nothing moved.
2. **Copy.** The fabric-primary, the one reader of every store, appends every line of every store on
   the old provider to the new one in source order, every writer's included, dead births' and
   deleted nodes' records among them, keeping a verified position per source file that advances only
   when the lines before it are durable on the new provider, with source order preserved per key
   across ownership changes as well as within one writer. Nodes keep writing the old provider
   throughout; nothing is blocked and nothing is written twice. Claims stay decided in the old
   provider.
3. **Freeze, tails, decision, release.**
   - *Freeze.* Each node first closes admission to ordinary appends and claim operations for the
     `change_id`, settles every already-issued append and claim operation to a resolved durable
     outcome, and durably records the frozen state and both providers in its data dir before
     acknowledging. An indeterminate operation prevents that acknowledgement. On restart the node
     reads this state before opening ordinary writes and resolves the same run's decision; a stale
     launch environment cannot unfreeze it. Migration control records use the explicit migration
     path while ordinary writes remain blocked. Each node holds every write
     it would make as owed. Nothing is refused and nothing is acknowledged before it is durable: a
     caller's command and a node's declaration are answered only once their line has landed, so the
     caller and the declarer retry until then; a fact a node observes, a connection event, is
     appended to a pending log in its own data dir and replayed at the release or at its restart. No
     claim is decided during the freeze.
   - *Tails.* Each frozen node records its final source positions and its own row, then acknowledges.
     The fabric-primary continues the ordered copy from its verified positions to those final
     positions, dead births' and deleted nodes' records included; nodes do not independently append unordered
     tails. Every writer must have acknowledged the freeze or be proven unable to write by node
     investigation's `exited` verdict. Silence is not proof and no timer substitutes for it.
     For each key, the destination history must be a prefix of the source history before missing
     records are appended. Existing operations are verified by identity and content, not merely by
     the highest identity or set membership. A hole or reordered history is refused by name before
     cutover; an older missing record is never appended behind a newer record of that key. A retry
     after a lost copy receipt recognizes the already-landed prefix and does not append it twice.
     The pass verifies the completed order and current value of every key, then transfers every held
     name claim and in-flight row with the exact holder and reads the two claim sets back equal.
   - *Decision.* The change's decision is one durable, mutually exclusive record on the new provider,
     `commit` or `abort`, keyed by `(build_id, change_id)` using the store's insert-and-fail primitive. The fabric-primary inserts
     `commit`, and only then appends the Fabric row's line on the new provider: the new provider
     current, the old appended to `previous_topology_storage_providers`. The insert is the commit
     point and the claim cutover. A successor, or the fabric-primary after a lost reply, learns the
     decision only by reading the record: `commit` found is committed and the row's line is written
     if missing; nothing found is undecided, and a revert is itself a decision, permitted only by a
     successful `abort` insert; a provider not answering is neither, and the freeze holds until it
     answers. A timeout never means "not committed".
   - *Release.* Each node swaps to the new provider, lands its owed writes there in its own order,
     answers its owed commands, and closes the old provider. Claims are decided in the new provider
     from here. A node that fails its release keeps its writes owed and retries.

While a change is undecided, a `build.create` naming the old provider is admitted beside it and
is its abort: the fabric-primary inserts `abort` for `(build_id, change_id)` and the revert below
follows. It is how the application or a person ends a freeze that a writer neither acknowledges nor
is proven `exited` from; nothing else ends it and no timer does. Once `commit` is inserted, that
Build is refused by name, `change-committed`, and the releases continue.

A failed step fails by name and fails the Build as any step does; before the decision a failure
reverts once `abort` is inserted, the freeze lifting on the old provider with the owed writes landing
there, the new provider closed and the Fabric row unchanged, nothing lost. After the decision only
releases remain, each an owed step a node retries, the Build in progress until the last.
`fabric.delete` during a change is admitted and interrupts the Build as any Build; the records on
both providers stay. The previous provider is kept, frozen at the commit and complete to the last
tail, named on the Fabric row, written no more and never deleted by RDM; removing it is the
application's own work or nobody's. A later Build naming it again copies into it only the lines it
lacks.

A caller reads the fabric's with `fabrics.get` and each node's with
`nodes.get [topology_storage_provider: file]`.

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
