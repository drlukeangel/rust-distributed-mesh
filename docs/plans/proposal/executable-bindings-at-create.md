## The executable a node runs: named per node at create, replaced only by re-birth

Proposal for i143 (the lead, 2026-10-24; revised by the architect the same day on Luke's rulings and
Architect 1's review of `b28db1bdb`). **Decided by Luke, 2026-10-24** (Q157): "bindings aren't attached
to the fabric they're attached to the node"; "it's rebirth only"; "create only"; the executable is named
per node in the request, not per type, so a type may run two executables at once; no stored default, the
caller always provides; the operation that landed as `614d01653..5e070d089`, `9fbf7efba` and its word are
gone (the word is banned: it did not say what it does). Being built after this text; the page sentences
land with the code.

An **executable binding** is an executable path, its sha256 and, under the container provider, the image
(`rdm-core/src/binding.rs`). "Executable" is the one word; "exe" is talk.

---

### 0. The rule

**A node's executable is an input of `node.create` and of nothing else; the birth runs it for its whole
life; a new executable is a new birth at the same `path.name`.**

1. **Named per node, in the request.** The accepted topology carries a typed config per path,
   `PathConfig { storage, placement }`; it gains `executable`. A Build request creates a node by naming
   it: its `path.name` and its config, executable included (Luke, 2026-10-24: today no request names a
   node, every change is by counts and the compiler mints the paths, `accepted.rs:459-464`, so a caller
   cannot attach an executable to a path it never named). Counts leave RDM's wire and stay at Rafka's
   door as its vocabulary (build.md §1, "product-facing input"), where the door expands a count into
   named paths with configs, reading the current topology for the ordinals. RDM mints a `path.name`
   only for a create it originates itself: recovery reuses the path, and the one-admin mesh's second
   admin (rule 5) takes the next free ordinal, as the compiler does today. There is no per-type section in RDM and no stored
   default anywhere: a created path whose config names no executable is refused by name at acceptance,
   nothing runs. The refusal is acceptance's, for new requests only: a Build decoded from the journal
   whose paths name none, every Build accepted before `614d01653` among them, is valid history with its
   births' rows as the record, and an admin restarted over an old store reads its current Build as it
   is, never as malformed. Per-type convenience is Rafka's door's: its `shape_bins` is per type and the door
   expands it into per-path entries before the typed call.
2. **Compiled at acceptance, explicit after it.** The fabric-primary compiles the request against the
   current topology; every created path's entry holds its executable and is never re-read from the
   request, as counts are never re-read (build.md §1). The accepted topology is the one durable answer
   to "what should this node run", beside the shape.
3. **Recorded on the row at the claim.** The create's name claim records the executable on the node's
   row (R6, landed `11faeb419`), the bootstrap admin's own included: it hashes the executable it is
   running and records that; there is no configured sha to hold it to (§2). The row is the record of
   what this birth runs.
4. **Re-hashed at the launch.** The launching admin hashes the file and launches only on a match; a
   missing file or a differing hash is refused by name and nothing else is launched in its place.
5. **Replaced only by re-birth.** A request that names a different executable on an existing path is a
   planned replacement, not a refusal: RDM realizes it as node.delete then node.create at the path, a
   new node.id and incarnation, in RDM's order with RDM's guards: a type's non-primary members before
   its primary, the primary's move first by node.primary.update, the fabric-primary's move ending the
   caller's stream `FabricPrimaryMoved` and the caller resubmitting the same build_id, R5's one-node type
   with no update, and the one-admin mesh (R4, Luke's arm A, kept): a mesh whose only node-admin is to be
   replaced first gets a second node-admin created on the new executable at a new `path.name` of the
   node-admin type in that mesh (the next free ordinal, `mesh1.admin.2`), ReadyForTraffic and
   mesh-primary by node.primary.update before the original is replaced, and the
   added admin stays in the topology, since deleting a mesh's only admin would leave its own create with
   no executor. A path whose birth is under node recovery is replaced after that recovery's attempt
   completes, never in the same attempt: one attempt in flight per mesh (R-BI7) and the planner's
   recompute per attempt order the two. The successor's storage follows the path's config: `Persistent` inherits the
   predecessor's data dir and its endpoint key (`RDM_INHERITED_DATA_DIR=1`), `Ephemeral` allocates a
   fresh one; its config and tags are inherited from the Deleted row (node-replace.md "Inherited"). A
   request that changes the path's storage or placement with its executable is one re-birth with the
   new config, since the entry is explicit; nothing is inferred. `node.restart` keeps the executable
   (node-restart.md "Kept"). Rafka submits the shape; it composes no steps.
6. **Recovered from the row.** Node recovery is a create that names no executable on its step, so it
   takes the row's (node-recovery.md "Retained"), never the recovering admin's and never the topology's
   next entry: a heal never silently upgrades; a pending replacement follows as its own step.
7. **Read from the records.** `NodeView` gains the executable from the row, so `node.get` and the
   fabric's per-node read return what each node runs beside status, config and tags.
   `fabric.nodes.get` gets its wire mapping as the mesh selector's answer over every mesh, from the
   rows, no filter. Nothing about executables rides gossip or `node.topology.get`.

Every Build that puts the fabric to pending completes only after its own ready-for-traffic round
(Q154); a replacement Build is one of them.

### 1. Removed, kept, corrected

- **Removed:** `BuildChange::Roll` and `TopologyChange::Roll` (their positional slots reserved, no format
  move, as the Status enums did); `FabricTopology.bindings` and its carry-forward and every read of it
  at launch (`runner.rs` `launch_set`); the removed operation's cells and `docs/plans/proposal/roll.md`;
  R6's check of the running executable against a configured `node_admin` sha, with its cell, since the
  configuration goes (§2).
- **Kept and moved to the replacement, renamed without the banned word:** the planner and its progress
  (`roll_operations`, `roll_progress`) are exactly rule 5's order, non-primaries first, the primary's move,
  the fabric-primary last, and the interrupted-delete resume of `9fbf7efba` is rule 5's resume; they
  become the replacement's planner, driven by each path's entry versus its row rather than by a fabric
  set. Nothing is deleted and rewritten.
- **Kept:** R6 (every row records what its birth runs); R5's page sentence (a type's only node needs no
  primary update; its predecessor half is one `node.delete`, never a standalone connections delete,
  which the executor refuses on a type's only node); Q154's generic hold for every Build that puts the
  fabric to pending (Blocked naming `fabric-change`), only the removed operation's branch of it goes.
- **Corrected on Architect 1's review:** a containerized admin loses the launch set that told it which
  directories to mount, so executable directories become the admin's launch configuration: read-only
  mounts at stable host paths, given at the admin's launch, and an executable outside them is refused
  by name at acceptance, before anything destructive (A1 gap 1). Compatibility, explicit (A1): the
  durable-format move's decoder fills each path's executable in an accepted Build written before the
  field from that Build's old per-kind set, by the path's kind, once, so the source is converted before
  it is removed, and that fill is the desired topology only, never a birth's record: the old set names
  the target, and after an interrupted change an unreplaced birth may run the previous executable. A
  row written before R6 decodes with none, and a birth whose row records no executable has no
  automatic recovery: node recovery of it is refused by name, since nothing birth-specific says what it
  ran (A1); the repair is a request naming that path's executable, the ordinary replacement, which
  deletes the birth and creates one whose row records what it runs. Never an inference from the
  recovering admin and never from the topology's entry. A topology edit that changes an
  existing node's executable is the replacement of rule 5, not a refusal (A1 gap 3, inverted on Luke's
  ruling).

### 2. Day 0

The bootstrap admin's day-0 shape is itself alone (`FabricTopology::root`), and R6 records its own
running executable. Every other node is created by a request that names its executable per path. So
`RDM_EXECUTABLE_BINDINGS` and `RDM_EXECUTABLE_CANDIDATE` carry nothing RDM needs: **both go**
(the architect, 2026-10-24, delegated by Luke: "bring the decision"). The bootstrap admin records the
executable it is running (R6); a configured sha to hold it to would be a claim about the same file. The
harness validates the entries of its own requests; the fabric-primary refuses a missing file or a wrong
sha by name at acceptance. A containerized admin's executable directories become its launch
configuration, `RDM_EXECUTABLE_DIRS`, comma-separated host directories mounted read-only at their host
paths, a row in CLAUDE.md's env table; an executable outside them is refused by name at acceptance. The
bootstrap admin reads it from its environment; the launching admin writes its own value into every
node-admin launch, as it writes the root, so every admin of the fabric mounts the same directories; as an
`RDM_*` name it is already in Q150's refused set for declared variables.
`RDM_BIN_DIR` stays only as where the harness and the bootstrap find `rdm-node-admin` itself, never a
launch source: the built-in mode that launched a kind's binary from `bin_dir` with no sha goes with the
file, since every create now names its executable. Priced (the dev): every estate that boots without a
set, `Estate::bootstrap`, most of rdm-test and the demo, relies on built-in mode, so every
`reconcile_shape` and create in those cells names executables per path, the harness expanding per-type
to per-path as Rafka's door does; hundreds of call sites, so it is its own landing with the gate's full
set, landed before the product change.

### 3. Pages that change: exact old and new text for Luke's yes, landing with the code

**3.1 New page, `docs/architecture/build-deployment.md`** (whole text):

```
# Build deployment: the executable a node runs

[Lifecycle index](fabric-mesh-node-lifecycles.md) · [Build](build.md) · [Deployment provider](deployment-provider.md)

build.md owns the engine: the accepted topology, attempts, claims and receipts. deployment-provider.md
owns the provider: how one runtime is launched, inspected and terminated. This page owns what a Build
deploys: the executable each node runs, from the request to the row and back out through the reads.

## 1. The rule

A node's executable is an input of node.create and of nothing else. The birth runs it for its whole
life. A new executable is a new birth at the same `path.name` (Q157).

## 2. From the request to the row

1. **Named per node.** A Build request that creates nodes names each path's config, and the config
   carries the executable: its path, its sha256 and, under the container provider, the image. There
   is no per-type default in RDM and no stored default anywhere. A created path whose config names
   no executable is refused by name at acceptance and nothing runs. A product's door may expand a
   per-type input into per-path entries before the typed call; RDM never sees a type-level default.
2. **Compiled at acceptance.** The fabric-primary compiles the request against the current topology.
   Every created path's entry in the accepted topology holds its executable, and the request is never
   re-read, as counts are never re-read (build.md §1). The accepted topology is the one durable answer
   to "what should this node run", beside the shape.
3. **Recorded at the claim.** The create's name claim records the executable on the node's row, the
   bootstrap admin's own included: it hashes the executable it is running. The row is the record of
   what this birth runs.
4. **Re-hashed at the launch.** The launching admin hashes the file before `DeployRuntime` and
   launches only on a match (deployment-provider.md). A missing file or a differing hash is refused by
   name; nothing is launched in its place.
5. **Reachable by the launcher.** A containerized admin mounts the executable directories its launch
   named (`RDM_EXECUTABLE_DIRS`), read-only at their host paths; an executable outside them is refused
   by name at acceptance, before anything destructive.

## 3. Changing a node's executable

A request that names a different executable for an existing path is a planned replacement. RDM
realizes it as node.delete then node.create at the path, a new node.id and incarnation, in its order
with its guards: a type's non-primary members before its primary, the primary's move first by
node.primary.update, the fabric-primary's move ending the caller's stream `FabricPrimaryMoved` and the
caller resubmitting the same build_id, a type's only node replaced with no update (node-primary-update.md
"No-successor cases"), and a mesh whose only node-admin is to be replaced first given a second node-admin
at a new `path.name` of that type on the new executable, ReadyForTraffic and mesh-primary, which stays. A path under node recovery is
replaced after that recovery's attempt completes, never in the same attempt. The successor's storage follows the path's config: `Persistent` inherits the
predecessor's data dir and endpoint key, `Ephemeral` allocates a fresh one; config and tags come from
the Deleted row (node-replace.md "Inherited"); a storage or placement change in the same request is the
same re-birth with the new config. The row then records the new executable. node.restart keeps the executable
(node-restart.md "Kept"); nothing changes a running birth's executable. A Build that puts the fabric
back to pending completes only after its own ready-for-traffic round (states.md).

## 4. Recovery

Node recovery is a create that names no executable on its step, so it takes the row's
(node-recovery.md "Retained"), never the recovering admin's and never the topology's next entry. A
heal never upgrades; a pending replacement follows as its own step. A row written before the field
existed decodes with none, and a birth whose row records no executable has no automatic recovery: the
topology's entry is the target, not what the birth ran, so recovery of it is refused by name; the repair
is a request naming that path's executable, the ordinary replacement.

## 5. Reads

The node's row is the source. `node.get` returns each node's executable beside its status, config and
tags (`NodeView.executable`); `fabric.nodes.get` is the mesh selector's answer over every mesh, from
the rows. The digest, the `Members` projection and `node.topology.get` carry no executable.

## 6. Refusals, by name

A created path with no executable; an executable outside the launcher's directories; a missing file;
a differing hash; node recovery of a birth whose row records no executable (repaired by a replacement
request naming it).
```

**3.2 build.md §1**, after "Counts may remain product-facing resize input, but the compiler resolves a
count change to exact paths before the Build is persisted." add:

```
Each path's config carries the executable the node runs ([build-deployment.md](build-deployment.md)); a request creates a node by naming its `path.name` and its config, the compiler holds the executable on the path's entry before the Build is persisted, and a created path with none is refused by name. Counts are the product's door's vocabulary, expanded into named paths before the typed call; RDM mints a `path.name` only for a create it originates itself. A request that names a different executable for an existing path changes that entry, and the reconciler realizes the change as that node's replacement.
```

**3.3 build.md §4.1**, in the block, old `complete explicit topology map` new `complete explicit topology map, each path's config and executable`.

**3.4 build.md §12**, after §12.4, a new case:

```
### 12.5 Replace a node's executable

~~~text
fabric-primary accepts Build B22:
    mesh2.rpc.north: executable A -> executable B
    B22 is the current Build
~~~

The topology names the same paths. mesh2.rpc.north's entry names executable B while its row records A,
so the reconciler replaces it: [node.delete](lifecycles/node-delete.md) then [node.create](lifecycles/node-create.md)
at the path, a new node.id, under mesh2's primary and the guards of [build-deployment.md §3](build-deployment.md).
A primary of any type moves first. Nothing else in the fabric changes.
```

**3.5 deployment-provider.md**, after "A launch that runs a bound executable re-hashes it before
`DeployRuntime` and launches it only when the hash matches its binding; ..." add:

```
Which executable a node runs, and how it is named, recorded, replaced and read, is [build-deployment.md](build-deployment.md)'s.
```

and in §2.7, a new bullet after **Network.**:

```
- **Executables.** A containerized node-admin mounts the executable directories its launch named (`RDM_EXECUTABLE_DIRS`), read-only at their host paths; an executable outside them is refused by name at acceptance ([build-deployment.md §2](build-deployment.md)).
```

**3.6 node-create.md**, the Spec row. Old:

```
| Spec | Executable/budgets, placement and whether the data dir is inherited; the one description of a node to create, never a fault-injection mechanism |
```
New:
```
| Spec | The path's config: its executable (path, sha256, image), placement, storage and whether the data dir is inherited; the one description of a node to create, never a fault-injection mechanism. The executable is the create's input and nothing else's: recorded on the row at the claim, re-hashed at the launch, never changed for the birth ([build-deployment.md](../build-deployment.md)) |
```

**3.7 node-replace.md**, the Successor row. Old:

```
| Successor | Created at `<name>` once the predecessor has released it: a re-birth, with a new node.id, incarnation and runtime; new port; key from its specified data dir |
```
New:
```
| Successor | Created at `<name>` once the predecessor has released it: a re-birth, with a new node.id, incarnation and runtime; new port; key from its specified data dir; its `Create` step names its executable ([build-deployment.md](../build-deployment.md)) |
```

**3.8 node-recovery.md**, the Retained row, append after "never the recovering admin's)":

```
 ; a row written before the executable field decodes with none and the node recovers once on the executable the current entry names for its path, the recovered row recording it
```

**3.9 states.md**, the section at line 174. Old heading and first sentences:

```
## Resize and roll

A resize or roll puts the fabric back to pending; meshes already serving stay ready-for-traffic. During a bin roll, a member still on its
old binary/identity does not satisfy its planned replacement. A new binary requires
a replacement node with a new NodeId and incarnation; node.restart retains the same
process and binary. The mesh cannot report
ready-for-traffic, and the fabric cannot climb, until the roll's last planned birth checks
in.
```
New:
```
## Resize and executable replacement

A resize or an executable replacement puts the fabric back to pending; meshes already serving stay ready-for-traffic. Its Build is Complete only when the fabric is ready-for-traffic again from this Build's own climb, as fabric.create's is: while the fabric climbs, the Build reports Blocked naming the climb, never Complete; a round or hook that fails fails the Build on that step. During an executable replacement, a member still on its old executable and identity does not satisfy its planned replacement. A new executable requires a replacement node with a new NodeId and incarnation ([build-deployment.md](../build-deployment.md)); node.restart retains the same process and executable. The mesh cannot report ready-for-traffic, and the fabric cannot climb, until the replacement's last planned birth checks in.
```
(The rest of the paragraph, on Rafka's state-sync answer, stands. This folds Q154's states.md sentence
in, so Q154's own sentence is not added twice.)

**3.10 node-rpc-envelope.md.** (a) The `BuildChange` block: `AddNode { mesh, node_kind }` becomes
`AddNode { node: PathName, config: PathConfig }`; `CreateMesh { mesh: MeshCounts }` becomes
`CreateMesh { mesh: String, nodes: Vec<(PathName, PathConfig)> }`; `ReconcileFabric` and `ReconcileMesh`
carry named nodes with configs the same way, their count forms reserved (positional, no format move);
`RemoveNode { node }` is already by name. After the `Recipe` comment, a comment:

```
    // Every create names its path's config, PathConfig { storage, placement, executable }: the
    // executable (path, sha256, image) the node runs, compiled onto the accepted topology's entry and
    // recorded on the row at the claim (build-deployment.md). A request naming a different executable
    // for an existing path is that node's replacement. Two slots after Restart are reserved (the
    // removed change variants; positional, no format move).
```
(b) The fabric reads row (line 328): old `fabric.nodes.get answers the fabric topology` new
`fabric.nodes.get answers every node of the fabric from the rows, the mesh selector's answer over every mesh, each with its status, config, tags and executable`.
(c) The `Nodes` reply note: `NodeView` gains `executable: Option<Binding>` from the row, on the
generated wire page with its fields.

**3.11 fabric-mesh-node-lifecycles.md**, after the Build row:

```
| [Build deployment](build-deployment.md) | what a Build deploys: the executable each node runs, named per node at create, recorded on the row, re-hashed at the launch, replaced only by re-birth, recovered from the row, read from the rows |
```

**3.12 CLAUDE.md env table**: remove nothing (the two `RDM_EXECUTABLE_*` variables have no row); add
`| RDM_EXECUTABLE_DIRS | _(unset)_ | rdm-node-admin under the container provider: comma-separated host directories mounted read-only at their host paths into a launched admin's container; an executable a Build names outside them is refused by name at acceptance. |`;
`RDM_BIN_DIR`'s row becomes `rdm-node-admin: where rdm-node-admin itself is found; never a launch source: every created node's executable is the Build's.`

**3.13 Records**: the PRD's R-D1 row (done, `e76fb7374`); the rulings index Q157 (done); rafka-v2's
i141 B3 packet and `needs-ownership-under-rdm.md` §2.4, for the lead.

### 4. Cells, RED first

- a node created with a named executable runs it and its row records it; a restart keeps it;
- a request naming a different executable on an existing path: a new node.id at the path running the new
  one, the old birth deleted first, in RDM's order; the fabric-primary's own replacement moving it once;
- a request replacing every node of a two-mesh fabric: Complete only after the ready-for-traffic round
  (Q154), Blocked naming the climb meanwhile;
- a one-broker mesh's broker replaced: one `node.delete` then the create, no refusal at any step;
- a one-admin mesh whose admin's executable changes: a second admin created first on the new executable,
  ReadyForTraffic and mesh-primary, then the original replaced, the added admin in the topology after;
- a recovery open on a path whose entry names a new executable: the recovery completes on the row's, then
  the replacement runs as the next attempt's step; never both in one attempt;
- an admin restarted over a store whose current Build predates the field: it reads it as valid history
  and serves;
- a containerized admin launched by another admin mounts the launching admin's `RDM_EXECUTABLE_DIRS`;
- a recovery runs the executable the lost birth's row records, with a newer entry pending on the path;
- a created path naming no executable refused by name at acceptance, nothing run; a missing file or a
  differing hash refused by name at the launch;
- a request that creates a node without naming its `path.name`, a count form, refused as unknown; a
  request naming `mesh1.rpc.3` with its config creates exactly that path; the harness's and the demo's
  shape helpers expand counts into named paths with configs before the typed call;
- a container fabric: an executable outside the admin's configured directories refused by name at
  acceptance; one inside, from a directory absent from day 0's input, launched;
- an executable-only replacement of a `Persistent` broker: the successor opens the predecessor's data dir
  with its endpoint key, config and tags inherited; of an `Ephemeral` gateway: a fresh data dir;
- an accepted Build in the old format: its entries filled from its old per-kind set at decode, the
  desired topology only; a row in the old format: node recovery of that birth refused by name, then
  repaired by a request naming its executable, whose successor's row records it;
- a legacy mixed-version fabric: the old set names B, an unreplaced birth with an old-format row runs
  A; its recovery refused by name, never re-created on B; the replacement request re-creates it on B
  by design;
- `node.get` and `fabric.nodes.get` return each node's executable beside its status; `node.topology.get`
  and the digest carry none;
- a `Roll` change refused as unknown; the Build wire's reserved slots decode as before.
