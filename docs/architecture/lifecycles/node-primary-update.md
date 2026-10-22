# Node primary update

[Lifecycle index](../fabric-mesh-node-lifecycles.md) · [States](states.md) · [Op contract](../node-rpc-envelope.md#primaryupdate-op-0x21-rdm) · [Elections](../fabric-node-lifecycle-elections.md)

`node.primary.update` moves the primary of the nodes of one type in one mesh, node-admins and every
other type alike, and completes as `node.primary.updated`. `node.fabric-primary.update` moves the
fabric-primary by the same primitive and completes as `node.fabric-primary.updated`. Commands only go
down: the fabric-primary commands a mesh-primary, and a mesh-primary commands the primaries of its
mesh's other node types. No node commands its own primary. A stop, delete, restart, replace or
`node.connections.delete { scope: All }` of a primary runs it first.

Who is primary is one fact with one writer, on one row
([the Mesh row](../node-identity-and-state.md#27-a-mesh-is-an-entity-and-node-admin-owns-it),
[the fabric row](../node-identity-and-state.md#28-a-fabric-is-an-entity-and-the-fabric-primary-owns-it)):

- the Mesh row (`meshes/<mesh>`) names the mesh-primary and each node-type primary of that mesh, each as
  its exact birth `(node_id, incarnation)` and its `revision`;
- the fabric row (`fabric/`) names the mesh whose mesh-primary is the fabric-primary. The mesh-primary
  and the fabric-primary move together: a same-mesh update of the mesh-primary is one Mesh row write and
  the fabric row does not change. They are separated in one case only, no node-admin of that mesh being
  available to the backbone ([fabric recovery](fabric-recovery.md)), or under mesh.delete of that mesh
  ([below](#the-fabric-primary-nodefabric-primaryupdate)).

`is_primary` and `is_fabric_primary` are projections of those rows, never written on a node row.

## Operation contract

| Field | Contract |
|---|---|
| Commander | Above the primary: the fabric-primary for a mesh-primary, the mesh-primary for every other node type of its mesh. The mesh-primary owns its nodes always: a command the fabric-primary originates for one of them reaches it as a forwarded command through the mesh-primary, never directly |
| Writer | For the `node-admin` type the successor writes the Mesh row naming itself, since it is that row's writer from then; for every other type the mesh-primary writes it (Q107 (1)). One writer per row, never two |
| The primary | Makes the `node.primary.update` call on its successor's exact birth |
| Roles | For the `node-admin` type the primary is the mesh-primary and the commander is the fabric-primary; the successor writes. For any other type the mesh-primary is commander and writer, and the primary only makes the call |
| The row | The one Mesh row write at the next revision, naming the successor. Before the row nothing has changed; after it the successor is primary and the ordinary rules apply. There is no path back |
| Both primaries | When the mesh-primary is also the fabric-primary and the successor is in the same mesh, the one Mesh row write makes the successor both, the fabric row unchanged |
| Precondition | Before the initial fabric first reaches ready-for-traffic, every call is refused `fabric-not-ready` ([States](states.md#fabric-not-ready)) |
| Completion | `node.primary.updated { revisions }`, answered by the successor on the primary's call; `node.fabric-primary.updated` for the fabric-primary |
| Wire | [PrimaryUpdate / PrimaryUpdated on 0x21](../node-rpc-envelope.md#primaryupdate-op-0x21-rdm) |

## Numbered end-to-end sequence

Mesh-primary NA1, successor NA2 for the `node-admin` type. For another type read "the mesh-primary" for
every writer step and B1/B2 for the primary and its successor; for a non-admin type the order of steps
1 and 5 swaps: the mesh-primary is writer and commander, so it pre-checks, announces and writes the row
first (steps 2-5), and its command to the primary B1 comes after the row and carries it; B1 then makes
the call of step 6. For the `node-admin` type the primary is commanded first and its successor writes.

1. The primary is commanded from above, as a step of the Build that stops, deletes, restarts, replaces or
   runs `node.connections.delete { scope: All }` on it.
2. The writer pre-checks from its own view: the election over the nodes of that type in the mesh with the
   primary excluded names a ReadyForTraffic successor. If it names none, the step is refused by name and
   nothing has changed, no row ([no-successor cases](#no-successor-cases)).
3. The mesh-primary, as sole author of member status, publishes "primary updating": operation, node type,
   the primary's and the successor's exact births, the current and next revisions. It states intent
   only; receivers keep the current primary until the row names the next revision.
4. For the `node-admin` type, NA1 finishes the writes it has issued before it calls NA2 (Q109): from
   NA2's row NA1 writes the mesh's rows no more. A non-admin primary has no mesh rows to finish writing
   (its own `connections/<node>/` rows are its own, R14).
5. The writer writes the one Mesh row at the next revision naming the successor. For the `node-admin`
   type this is NA2, on NA1's call of step 6; for every other type it is the mesh-primary, before its
   command. From this write the successor is primary and the old primary is not.
6. The primary calls `node.primary.update` on the successor's exact birth. The successor installs itself
   as primary (for the `node-admin` type by writing the row, step 5; for every other type by reading the
   row the call carries) and answers `node.primary.updated { revisions }` on that call. A non-admin
   primary then answers its commander's command.
7. The mesh-primary publishes "primary updated" with the same identifiers. A late "updating" never undoes
   an "updated": the row's revision, per node type, is these two frames' order key in the one gossip
   receive path (R-RX3); nothing orders inside a handler.
8. Only then does the commanding Build continue, and only once the successor's installation is
   acknowledged. The successor is now the old primary's primary (for the `node-admin` type, its
   mesh-primary) and commands the old primary's connections delete, stop, restart or replace.

## Failure contract

- **Refused before the row** (step 2, or the writer's own pre-check of the call's revision): refused by
  name, nothing changed, the Build's step fails by name.
- **The row write fails** (step 5). The row is a local write, temp file, fsync, rename (R14's fs
  contract), and it answers a named error or succeeds. A named error is a confirmed non-write: the step
  fails by name naming the error, and the old primary is primary under the unchanged revision. The one
  uncertain case is the writer dying before the rename; it resolves it itself on restart by reading the
  row (written: the successor is primary; absent: the old primary is, under the unchanged revision).
- **After the row, the successor is primary whatever the call returns.** The receiver never refuses the
  call for the birth it names (R-UC1a, R-RX5); it installs from the row and answers, and whether it is
  primary is a projection of the row against its own birth. A call that reached a process other than the
  named birth, a refusal or no answer decides nothing about the successor's birth (R-SW1: a failed probe
  never proves anything). The call fails by name, and [node investigation](node-recovery.md) on the
  successor's exact birth decides (Q111): reached, the call is retried against it; any other verdict, the
  successor is re-created and the ordinary election names the primary by the staleness-floor rule, the
  old primary included. No row is rewritten by this step.
- **The successor is unheard after the row.** When node investigation's verdict is anything but reached,
  the step fails by name, `the successor is unheard after the row: <verdict>`, and the Build is Failed
  (R-BI4). The ordinary election then names whoever it names, the old primary included: the lowest
  ReadyForTraffic node of that type in the mesh writes the Mesh row at the next revision first and
  publishes "primary updated" second. The successor's node goes through
  [node recovery](node-recovery.md) like any unheard member. A caller's resubmit opens a new run, so a
  new update under its own key, which excludes the old primary.
- **The commanded arm.** When a node.primary.update the fabric-primary commanded ends, the old primary
  answers it with the Build carrier's terminal, `Failed { step, reason }` on op `0x21`; `Incomplete` is
  never used for a step that ended. The fabric-primary fails the step on that variant, never on the
  reason's text. The reason records the step, the successor's node id and incarnation, the revisions the
  row moved, who the ordinary election named, and the investigation's verdict on the successor. The
  fabric-primary claims the attempt at the claim door before it commands.
- **Indeterminate after the row:** the row resolves it. An identical retry of the call replays the
  written result before any revision check; a call with a conflicting payload for the same operation is
  refused by name.
- **The old primary lost after the row, before "updated":** the successor and every reader take the
  primary from the row; the next writer to read the row publishes "updated". Completion needs neither
  gossip delivery nor the old primary's survival.
- **Discovery is not installation.** A later attempt that reads the row has discovered the update, not
  installed it. The resuming executor installs from the row locally and obtains the successor's
  `node.primary.updated` (its installation acknowledgement) before it commands the departing primary's
  destructive step. A successor node investigation found unheard is handled above.
- The workflow continues only after the result is installed and acknowledged: `node.primary.updated`
  answered to the resuming executor's call, never a read alone.

<a id="no-successor-cases"></a>

## No-successor cases

- **No other node of the type in the mesh:** `node.primary.update` commanded for a type with no other
  node of that type in the mesh is refused by name, `cannot transfer: no secondary of type <node_type> in
  mesh <mesh>`, until a secondary is available. The primary stays primary, no row is written, and the
  workflow that commanded it (the stop, restart or delete of that node) fails by name with that reason.
  The one exception: the delete of a mesh's only node skips the update. A mesh's sole `broker` cannot be
  stopped or restarted until a second broker is ReadyForTraffic. No row and no vacancy inferred from a
  stop.
- **No other mesh-primary for the fabric-primary:** the fabric arm's own refusal,
  [below](#the-fabric-primary-nodefabric-primaryupdate).
- **The last node-admin of a mesh:** mesh.delete's path, and mesh.delete never updates the mesh-primary.
  The mesh-primary stays mesh-primary and keeps publishing through the whole teardown, and a separate
  node.delete with the fabric-primary as executor deletes it last ([mesh.delete](mesh-delete.md)). If
  that mesh-primary is the fabric-primary, the fabric-primary moves first by `node.fabric-primary.update`
  with the departing mesh excluded; nothing else moves.
- **Fabric shutdown:** fabric.delete moves no primary; every primary stays until deletion.

<a id="fabric-seat"></a>

## The fabric-primary: node.fabric-primary.update

The fabric-primary is its own property, projected as `is_fabric_primary` (never a settable field: a
published bit is a projection of the fabric row and the Mesh row), with its own op,
`node.fabric-primary.update`. It runs the same sequence through the same primitive and completes as
`node.fabric-primary.updated` (never `node.primary.updated`, which is the mesh-primary's), the
fabric-primary (a mesh-primary by definition) as caller, its commander being the workflow that stops,
restarts, deletes or runs `node.connections.delete { scope: All }` on it, or mesh.delete. Only the
successor search and the row differ.

Successor search for the commanded transfer:

1. the fabric-primary's own mesh: the lowest ReadyForTraffic node-admin NodeId there, where the
   fabric-primary stays by preference;
2. only when that mesh has none, or it is the departing mesh under mesh.delete: the lowest NodeId among
   the other ReadyForTraffic mesh-primaries;
3. **neither found: refused by name**, `cannot transfer fabric-primary: no other node-admin
   ReadyForTraffic in mesh <mesh>, no other mesh-primary ReadyForTraffic`, naming both searches and what
   each found. The fabric-primary stays fabric-primary, no row is written, and the commanding workflow
   fails with that reason until a successor is available.

Where the refusal bites: a single-mesh fabric with one node-admin cannot stop, restart or delete its
fabric-primary until a second node-admin is ReadyForTraffic; mesh.delete of the only mesh, or of the
fabric-primary's mesh when no other mesh has a Ready primary, is refused the same way, and deleting the
last mesh is fabric.delete's path.

The ordinary election is untouched by this search: when the fabric-primary is unheard past the staleness
floor, the fabric-primary waits while only Pending admins of its mesh survive, and leaves the mesh only
when no node-admin of the mesh is available to the backbone ([fabric recovery](fabric-recovery.md)). The
search above is for the commanded transfer only.

**The row.** A successor in the same mesh (search step 1): the one Mesh row write names it mesh-primary,
and the fabric row, naming that mesh, makes it fabric-primary at the same boundary. A successor in
another mesh (search step 2): the fabric-primary writes the fabric row once, naming the successor's
mesh; under mesh.delete the departing mesh's mesh-primary stays mesh-primary until its own node.delete.
No other row records the move; every reader reads the Mesh row and the fabric row and folds nothing.

## Command, completion and gossip messages

```text
commander → primary       (a step of the Build that stops, deletes, restarts, replaces or deletes the primary's connections)
writer                    pre-check: a ReadyForTraffic successor of the type in the mesh, the primary excluded
mesh-primary → mesh channel, backbone   "primary updating" (operation, node type, both exact births, revisions)
primary                   issued writes finished (node-admin type)
primary → successor       node.primary.update (PrimaryUpdate)
writer                    the one Mesh row at the next revision (the successor for the node-admin type, on this call; the mesh-primary otherwise, before its command)
  ← node.primary.updated { revisions }   (PrimaryUpdated)
mesh-primary → mesh channel, backbone   "primary updated" (same identifiers)
commander's Build continues under the successor
```

"Primary updating" and "primary updated" are announcements, never the fact: the Mesh row is the fact.
They are best-effort dissemination, and a stop does not wait for their delivery (R-NS2): the
mesh-primary publishes a written fact as its author.

## Mermaid sequence diagram

```mermaid
sequenceDiagram
    participant FP as Fabric-primary (commander)
    participant NA1 as NA1 (mesh-primary)
    participant NA2 as NA2 (successor, writer)
    participant G as Mesh channel and backbone
    FP->>NA1: Build step on NA1 (stop, delete, restart, replace, connections delete)
    Note over NA1: pre-check: a ReadyForTraffic successor, NA1 excluded
    NA1->>G: primary updating
    Note over NA1: issued writes finished
    NA1->>NA2: node.primary.update
    Note over NA2: the one Mesh row at the next revision, naming NA2
    NA2-->>NA1: node.primary.updated { revisions }
    NA2->>G: primary updated
    Note over FP,NA2: the Build continues; NA2 commands NA1's destructive step
```

## Observability contract

Trace propagation, step spans and correlation follow the
[runtime spans](../node-rpc-envelope.md#runtime-spans). The update's spans carry the operation, the node
type, both exact births and the revisions: the "primary updating" and "primary updated" announcements,
the Mesh row's write, the successor's install, and every refusal by its typed reason.
