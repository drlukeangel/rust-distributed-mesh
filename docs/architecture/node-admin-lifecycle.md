# Node-admin lifecycle

[Lifecycle index](fabric-mesh-node-lifecycles.md) · [States](lifecycles/states.md) · [Op contracts](node-rpc-envelope.md#the-op-ledger)

This page owns the node-admin launch contract and its start modes, who starts a node-admin of an
existing mesh, and Rafka's recovery copy. One start, a handful of parameters; the sequences
themselves are [a node-admin joining a live mesh](lifecycles/mesh-investigation.md#a-node-admin-joins-the-live-mesh) and
[fabric recovery](lifecycles/fabric-recovery.md).

**Related:** [`fabric-mesh-node-lifecycles.md`](fabric-mesh-node-lifecycles.md) (every node
workflow node-admin drives, its events and its gossip),
[`fabric-node-lifecycle-elections.md`](fabric-node-lifecycle-elections.md)
(the primaries), [`build.md`](build.md) (the current Build, Builds).

---

## 1. Rules

1. **Only node-admins create nodes.** Every node of a fabric, node-admins included, is created by a
   node-admin executing a Build, except the Day-0 node-admin (§3) and a node-admin started by a
   person after total loss (§4.4).
2. **The node-admins are never all down.** A node-admin is replaced one at a time within a mesh.
   Different meshes may each replace one node-admin at the same time. Every mesh therefore keeps a
   running node-admin through any planned operation except mesh.delete, after whose
   mesh.deleted a separate node.delete removes the mesh-primary, and fabric.delete.
3. **The fabric primary is handed off, never wiped out.** Before its delete, stop or replace, it runs
   [node.primary.update](lifecycles/node-primary-update.md), commanded from above, and its successor, now
   its primary, commands the step ([elections §4.4](fabric-node-lifecycle-elections.md#44-explicit-committed-fabric-handover));
   as its mesh's last admin it stops being fabric-primary only by node.fabric-primary.update inside mesh.delete.
4. **Silence is not departure, and a failed connection is not death.** What became of an unheard
   node is [node investigation](lifecycles/node-investigation.md)'s verdict (Q111): reached, it is commanded
   back; anything else, it is re-created. A member whose node-admins are lost is never re-created
   because of that loss alone.
5. **Stored topology is a recovery contact map.** The attached snapshot is the last ready
   fabric, not the latest state. It locates live nodes; current topology comes from those nodes.
   A stored member may not answer, and the stored map is not current birth or liveness proof.
6. **A node-admin acts on its own view.** Silence is receipt age on the node's own clock, so a node held
   or suspended wakes to a view in which every member is silent at once; what it then does is what every
   node-admin does with an unheard member, [node investigation](lifecycles/node-investigation.md), whose
   verdict decides. A node-admin that was re-created while it was held is refused at admission and
   terminated when it comes back (Q110).

## 2. One start, its launch inputs

Every node-admin process starts the same way. What it does is decided by its parameters, carried
in the launch contract (`rafka_mesh_entity::launch`):

| parameter | launch field | meaning |
|---|---|---|
| mesh | `RDM_MESH_ID` | the MeshId the node-admin belongs to; its topology gossip channel is derived from it. **Absent with no durable topology: Day 0** (§3); durable topology selects recovery. |
| seed | `RDM_SEEDS` | one or more live nodes, `(EndpointId, address)`, the node-admin dials at once. It never waits to be heard first. |
| mesh primary | `RDM_MESH_PRIMARY` | this node-admin starts as its mesh's primary node-admin (the first node-admin created into a live mesh whose node-admins are all lost). |
| fabric primary | `RDM_FABRIC_PRIMARY` | this node-admin starts as the fabric primary (total-loss recovery). |

The fabric's store root (`RDM_STORE_DIR`) is required: every node of the fabric reaches the same
root, laid out `<store_dir>/<fabric_id>/{layout, fabric.jsonl, meshes.jsonl, nodes.jsonl, connections.jsonl, builds.jsonl, locks/}` (R-SR1; a birth's runtime row is a `node-runtime` line in `nodes.jsonl`),
and a missing or unreadable root fails the boot by name; nothing falls back to the data dir.
The node-admin's own data dir (`RDM_DATA_DIR`) holds only its runtime material (endpoint key,
identity, runtime record) and the recovery copy (§7): its attached last-ready snapshot, a
recovery contact map. Only a person-started recovery reads that copy. Without attached storage,
a recovery starts from a seed: any live node serves
[GetTopology](node-rpc-envelope.md#topology-op-0x1e-rdm).

`RDM_STOPPED_NODE_TTL` is the [stopped-node collector](node-identity-and-state.md#stopped-node-garbage-collector)'s
ttl, read by the fabric-primary only.

The two primary flags state what the node-admin must recover, not a role it keeps: once it holds
the current topology, whether it is a primary is the [election](fabric-node-lifecycle-elections.md)'s.

## 3. Start modes

The parameters select one of these modes:

| mode | parameters | started by | what the node-admin does |
|---|---|---|---|
| **Day 0** | no mesh and no durable topology | a person | Starts the fabric as its fabric primary ([fabric.create](lifecycles/fabric-create.md)), then waits for the next node or mesh deployment request (a Build). |
| **Create** | mesh, seed | a node-admin executing a Build | Runs [node.start](lifecycles/node-start.md) with FirstStart, its maker the accepting primary; once ready it takes part in elections. |
| **Recover** | mesh, seed, the exited node's data dir | a node-admin executing the recovery Build | node.create at the same path.name over the inherited data dir: same endpoint key, new NodeId, incarnation and port ([node recovery](lifecycles/node-recovery.md)). |
| **Create as mesh primary** | mesh, seed, mesh primary | the fabric primary | A live peer mesh lost all its node-admins: node.create of a node-admin into it ([mesh investigation](lifecycles/mesh-investigation.md#mesh-investigation) step 6), then the entry sequence (§4) and the mesh's missing node-admins (§4.3). |
| **Fabric recovery** | mesh, seed, mesh primary, fabric primary | a person | Every node-admin is lost (catastrophic): the entry sequence (§4), then every mesh's missing node-admins. The launched node-admin is a new node over its inherited data dir: new node.id and incarnation, same path.name and endpoint key, new port. |

node.stop and node.start launch nothing: the same process stops and rejoins
([node stop](lifecycles/node-stop.md), [node start](lifecycles/node-start.md)).

## 4. A node-admin enters an existing mesh

A node-admin created into a live mesh is node.create's
[mesh-present arm](lifecycles/node-create.md#mesh-absent) with the mesh's own MeshId: it joins the
mesh and never mints its id. It runs as an attempt of the SAME accepted Build. The steps are
[a node-admin joining a live mesh](lifecycles/mesh-investigation.md#a-node-admin-joins-the-live-mesh) and
[fabric recovery](lifecycles/fabric-recovery.md).

### 4.1 Maker admission and current topology

[A node-admin joins the live mesh](lifecycles/mesh-investigation.md#a-node-admin-joins-the-live-mesh) steps 1–3.

### 4.2 Become Ready and sweep the own mesh

[A node-admin joins the live mesh](lifecycles/mesh-investigation.md#a-node-admin-joins-the-live-mesh) steps 4–5; a
member the sweep did not reach goes to [node recovery](lifecycles/node-recovery.md).

### 4.3 Restore missing node-admins

[A node-admin joins the live mesh](lifecycles/mesh-investigation.md#a-node-admin-joins-the-live-mesh) step 6; the
order across meshes is [fabric recovery](lifecycles/fabric-recovery.md)'s.

### 4.4 Fabric-primary recovery and who starts it

The sequence a launched fabric-primary runs from its durable map is
[fabric recovery](lifecycles/fabric-recovery.md); the durable topology is a contact map (rule 5).

| event | who starts the node-admin | flow |
|---|---|---|
| a live peer mesh loses every node-admin | fabric-primary, same accepted Build, the mesh's own MeshId | node.create into the live mesh, maker admission/read, local topology read, own-mesh sweep, node recovery, restore own admins |
| fabric-primary is launched again into its own mesh | a person's launch on its durable data dir; the launched node-admin is a new node over the inherited data dir (new node.id, incarnation and port; same path.name and endpoint key); a launch that finds a living admin before its first mutation is refused NameHeld, never an ordinary admin at the primary's name (Q156, Q199) | no maker; durable-map topology read, own mesh first, same entry flow |
| the fabric-primary's mesh loses every node-admin while other meshes live | [fabric recovery](lifecycles/fabric-recovery.md), run by the backbone's lowest Ready mesh-primary from mesh investigation's decision step, exactly as a node-admin of the mesh would; same accepted Build, the mesh's own MeshId | no node-admin takes a primary role; the fabric is degraded; the node-admin at the fabric-primary's path.name is re-created (inherited data dir when `exited`, else a new one, R-FM2) and is mesh-primary and fabric-primary when Ready |
| every node-admin of every mesh is lost, nothing alive included ([fabric recovery](lifecycles/fabric-recovery.md#nothing-alive)) | person or script: the adoption launch at the lost fabric-primary's path.name, with no launcher and no primary flag, given the fabric id, its mesh and the store root, over the lost fabric-primary's data dir or a new one (Q156; the [runbook](../eng/fabric-recovery-runbook.md)); or a launch on the fabric-primary's data dir with mesh, seed and both primary flags | no Day 0; no maker; the adoption launch claims the fabric-recovery attempt of the current Build before any other write and is restored as mesh-primary and fabric-primary by step 4 once Ready; durable-map reads while a node is alive, own mesh first, then [mesh investigation](lifecycles/mesh-investigation.md#mesh-investigation) of the other meshes |

## 5. Join admission and topology reads

A created node-admin joins through its maker and reads topology as
[node start](lifecycles/node-start.md) defines; one joining a live mesh adds the local topology read of
[that sequence](lifecycles/mesh-investigation.md#a-node-admin-joins-the-live-mesh). An already-held
member's top-up is [node topology get](lifecycles/node-topology-get.md), and held-member repair is
[gossip.md §6](gossip.md#6-repair-held-member-stale-coverage-not-zero-neighbours). The op fields are
[JoinNode](node-rpc-envelope.md#join-op-0x1d-rdm) and
[GetTopology](node-rpc-envelope.md#topology-op-0x1e-rdm).

### 5.1 Declaration delivery and primary reports

A node's status report is [node status](lifecycles/node-status.md#node-status-declaration); a new
mesh-primary's republication of its adopted status is
[mesh investigation](lifecycles/mesh-investigation.md#mesh-investigation) step 7.

The Pending hand-off to a mesh's first admin is a row of [States: who sets each state](lifecycles/states.md#who-sets-each-state). Op fields are the [Status schema](node-rpc-envelope.md#status-op-0x1b-rdm).

## 6. Replacing node-admins

- node.stop then node.start (same NodeId, incarnation, endpoint and port) and replacement (new NodeId at the
  same path.name) of a node-admin are Builds, one node-admin at a time per mesh (rule 2).
- A Build that runs `node.connections.delete { scope: All }` on, stops or replaces a
  primary runs [node.primary.update](lifecycles/node-primary-update.md) first: the primary, commanded from
  above, hands over to its successor, who then commands the old primary's step (rule 3).

### 6.1 Named-birth replace input

Replacement intent is submitted through build.create to the fabric-primary over
[0x20](node-rpc-envelope.md#build-op-0x20-rdm). Accepted intent resolves the
named exact birth, rather than a heard view. The fabric-primary claims and dispatches
the run to executor_for's executor, which records the won claim before execution.

A replace of a birth absent from the view runs node.delete against its exact
runtime, rebuilt from the Build action and the data-root runtime record. It emits
`rdm.node_admin.node.update.via-rebuilt-birth` and refuses by name when no record names
the birth. Completion requires the NodeDeleted receipt of that exact birth. Source:
RDM's `rebuild_absent_birth` in `rdm-admin`.

## 7. Rafka storage and recovery

Inside Rafka, RDM's records live in the store Rafka's storage backend opens. RDM standalone keeps
the same records in the fabric's one store root (`store_dir`, §2): each writer writes its rows
there from Day 0 (Q171).
Every node-admin also keeps an on-disk copy of its fabric, meshes, nodes, connections,
status and Build records. That copy is a snapshot taken when the fabric last reaches
ready, not a continuously overwritten latest-state copy: the latest state
can contain the change that brought the system down, so a recovery from it can loop.
The next ready state replaces the on-disk snapshot.

Only a person-started recovery with that storage attached reads the snapshot.
An election or live hand-over uses held current records, not this recovery copy.
Without attached storage, recovery contacts a seed node and reads GetTopology; any live
node serves that read. A node-admin joining a live mesh calls its maker for join admission and topology
under §4.1. Stored, unversioned topology is only a contact map to a live own-mesh node,
which supplies the current versioned snapshot. Stored topology never refreshes liveness.
