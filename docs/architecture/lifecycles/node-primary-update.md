# Node primary update

[Lifecycle index](../fabric-mesh-node-lifecycles.md) · [States](states.md) · [Op contract](../node-rpc-envelope.md#primaryupdate-op-0x21-rdm) · [Elections](../fabric-node-lifecycle-elections.md)

`node.primary.update` moves the primary of the nodes of one type in one mesh, node-admins and every
other type alike, and completes as `node.primary.updated`. `node.fabric-primary.update` moves the
fabric-primary by the same primitive and completes as `node.fabric-primary.updated`. Commands only go
down: the fabric-primary commands a mesh-primary, and a mesh-primary commands the primaries of its
mesh's other node types. No node commands its own primary. A stop, delete, restart, replace or
`node.connections.delete { scope: All }` of a primary runs it first.

Who is primary is the [election](../fabric-node-lifecycle-elections.md)'s result, computed by every
node-admin from the facts it holds. Becoming primary and recording it are separate: the successor
installs itself from the election and the mesh-primary announces the change; the mesh-primary, as the
one writer of its mesh's rows ([node-identity-and-state.md §2.7](../node-identity-and-state.md#27-a-mesh-is-an-entity-and-node-admin-owns-it)),
records it as `is_primary` on the node's row, and as `is_fabric_primary` on a node-admin's own row for the
fabric-primary. **The write records the role; it does not grant it.** The mesh-primary and the
fabric-primary move together: a same-mesh successor becomes both, and records both on its own row. They
are separated in one case only, no node-admin of that mesh being available to the backbone
([fabric recovery](fabric-recovery.md)), or under mesh.delete of that mesh
([below](#the-fabric-primary-nodefabric-primaryupdate)). No other record of who is primary exists: no
seat row, no move log, nothing to fold.

## Operation contract

| Field | Contract |
|---|---|
| Commander | Above the primary: the fabric-primary for a mesh-primary, the mesh-primary for every other node type of its mesh. The mesh-primary owns its nodes always: a command the fabric-primary originates for one of them reaches it as a forwarded command through the mesh-primary, never directly |
| Writer | The mesh-primary records the change on the node's row: for the `node-admin` type that is the successor, NA2, which is the mesh's writer from the moment it is mesh-primary and records its own row (Q107 (1)); for every other type the mesh-primary records the node's row. A broker-primary writes nothing |
| The primary | Makes the `node.primary.update` call on its successor's exact birth |
| Roles | For the `node-admin` type the primary is the mesh-primary and the commander is the fabric-primary; the successor writes. For any other type the mesh-primary is commander and writer, and the primary only makes the call |
| The record | `is_primary` (and `is_fabric_primary`) at the next revision on the successor's row, written by the mesh-primary after the successor installed itself. The record follows the role; it never makes it. There is no path back |
| Both primaries | When the mesh-primary is also the fabric-primary and the successor is in the same mesh, the successor becomes both and records both on its own row |
| Precondition | Before the initial fabric first reaches ready-for-traffic, every call is refused `fabric-not-ready` ([States](states.md#fabric-not-ready)) |
| Completion | `node.primary.updated { revisions }`, answered by the successor on the primary's call; `node.fabric-primary.updated` for the fabric-primary |
| Wire | [PrimaryUpdate / PrimaryUpdated on 0x21](../node-rpc-envelope.md#primaryupdate-op-0x21-rdm) |

## Numbered end-to-end sequence

Mesh-primary NA1, successor NA2 for the `node-admin` type. For another type read B1/B2 for the primary
and its successor and "the mesh-primary" for the commander, the announcer and the writer: the order is
the same, command, then the successor becomes primary and announces, then the record. Becoming primary
and recording it are separate for every type.

1. The primary is commanded from above, as a step of the Build that stops, deletes, restarts, replaces or
   runs `node.connections.delete { scope: All }` on it.
2. The writer pre-checks from its own view: the election over the nodes of that type in the mesh with the
   primary excluded names a ReadyForTraffic successor. If it names none, the step is refused by name and
   nothing has changed, no row ([no-successor cases](#no-successor-cases)).
3. The mesh-primary, as sole author of member status, publishes "primary updating": operation, node type,
   the primary's and the successor's exact births, the current and next revisions. It states intent
   only; receivers keep the current primary until "primary updated" names the next revision.
4. For the `node-admin` type, NA1 finishes the writes it has issued before it calls NA2 (Q109): once
   NA2 is mesh-primary NA1 writes the mesh's rows no more. A non-admin primary has no mesh rows to finish writing
   (its own `connections/<node>/` rows are its own, R14).
5. The primary calls `node.primary.update` on the successor's exact birth. The successor becomes
   primary: it installs the election's result the call carries in its own topology view, answers
   `node.primary.updated { revisions }` on that call, and announces the change in its own digest
   (`is_primary`, and `is_fabric_primary` when both moved, at the next revision). A non-admin primary
   then answers its commander's command. For the `node-admin` type NA2 is the mesh's writer from this
   moment.
6. The mesh-primary records the change it hears: `is_primary` (and `is_fabric_primary`) at the next
   revision on the successor's row. For the `node-admin` type that is NA2 recording its own row; for
   every other type the mesh-primary records the successor's row from the successor's digest, the way it
   records every member fact it hears. The write records the role; it does not grant it.
7. The mesh-primary publishes "primary updated" with the same identifiers. A late "updating" never undoes
   an "updated": the row's revision, per node type, is these two frames' order key in the one gossip
   receive path (R-RX3); nothing orders inside a handler.
8. Only then does the commanding Build continue, and only once the successor's installation is
   acknowledged. The successor is now the old primary's primary (for the `node-admin` type, its
   mesh-primary) and commands the old primary's connections delete, stop, restart or replace.

## Failure contract

- **Refused before the row** (step 2, or the writer's own pre-check of the call's revision): refused by
  name, nothing changed, the Build's step fails by name.
- **The record's write fails** (step 5 or 6). The row is a local write, temp file, fsync, rename
  (R14's fs contract), and it answers a named error or succeeds. The role is the election's, not the
  write's: a named error fails the step by name, the successor stays what the election names it, and
  the mesh-primary records the row on its next write. A writer dying before the rename changes nothing:
  on restart it reads its rows, computes the election from what it holds and records what is missing.
- **After the successor installed itself, it is primary whatever the call returns.** The receiver never refuses the
  call for the birth it names (R-UC1a, R-RX5); it installs from the election's result the call carries
  and answers, and whether it is primary is the election's result against its own birth, never the
  record's. A call that reached a process other than the
  named birth, a refusal or no answer decides nothing about the successor's birth (R-SW1: a failed probe
  never proves anything). The call fails by name, and [node investigation](node-recovery.md) on the
  successor's exact birth decides (Q111): reached, the call is retried against it; any other verdict, the
  successor is re-created and the ordinary election names the primary by the staleness-floor rule, the
  old primary included.
- **The successor is unheard after the update.** When node investigation's verdict is anything but
  reached, the step fails by name, `the successor is unheard after the update: <verdict>`, and the Build
  is Failed (R-BI4). The ordinary election then names whoever it names, the old primary included: the
  lowest ReadyForTraffic node of that type in the mesh is primary, "primary updated" is published and the
  mesh-primary records it. The successor's node goes through
  [node recovery](node-recovery.md) like any unheard member. A caller's resubmit opens a new run, so a
  new update under its own key, which excludes the old primary.
- **The commanded arm.** When a node.primary.update the fabric-primary commanded ends, the old primary
  answers it with the Build carrier's terminal, `Failed { step, reason }` on op `0x21`; `Incomplete` is
  never used for a step that ended. The fabric-primary fails the step on that variant, never on the
  reason's text. The reason records the step, the successor's node id and incarnation, the revisions the
  row moved, who the ordinary election named, and the investigation's verdict on the successor. The
  fabric-primary claims the attempt at the claim door before it commands.
- **Indeterminate after the update:** the election resolves it. An identical retry of the call replays the
  same result before any revision check; a call with a conflicting payload for the same operation is
  refused by name.
- **The old primary lost after the update, before "updated":** the successor and every reader take the
  primary from the election; the successor announces its role in its digest, and the mesh-primary (the
  successor itself for the `node-admin` type) records it and publishes "updated". Completion needs
  neither gossip delivery nor the old primary's survival.
- **Discovery is not installation.** A later attempt that reads the record has discovered the update, not
  installed it. The resuming executor installs from the election locally and obtains the successor's
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

## The fabric-primary: node.fabric-primary.update

The fabric-primary is its own property, projected as `is_fabric_primary` (never a settable field: a
published bit is a projection of the election's result, recorded on the node-admin's own row by itself
as its mesh's writer), with its own op,
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

**The record.** A successor in the same mesh (search step 1) becomes mesh-primary and fabric-primary
at the same boundary and records both on its own row. A successor in another mesh (search step 2)
becomes fabric-primary and records `is_fabric_primary` on its own row, as its mesh's writer; the
departing fabric-primary records its own row with it cleared, and under mesh.delete stays mesh-primary
until its own node.delete. The fabric row (`fabric/`) is the fabric's identity and never changes for
this. No other record exists; every reader computes the election from the facts it holds and folds
nothing.

## Command, completion and gossip messages

```text
commander → primary       (a step of the Build that stops, deletes, restarts, replaces or deletes the primary's connections)
writer                    pre-check: a ReadyForTraffic successor of the type in the mesh, the primary excluded
mesh-primary → mesh channel, backbone   "primary updating" (operation, node type, both exact births, revisions)
primary                   issued writes finished (node-admin type)
primary → successor       node.primary.update (PrimaryUpdate)
successor                 becomes primary: installs the election's result in its topology view; announces it in its digest
  ← node.primary.updated { revisions }   (PrimaryUpdated)
mesh-primary              records is_primary at the next revision on the successor's row from what it hears (NA2 its own row)
mesh-primary → mesh channel, backbone   "primary updated" (same identifiers)
commander's Build continues under the successor
```

"Primary updating" and "primary updated" are announcements, never the fact: the election's result is the fact, and the row records it.
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
    Note over NA2: becomes primary: installs the election's result; announces it in its digest
    NA2-->>NA1: node.primary.updated { revisions }
    Note over NA2: records is_primary on its own row at the next revision
    NA2->>G: primary updated
    Note over FP,NA2: the Build continues; NA2 commands NA1's destructive step
```

## Observability contract

Trace propagation, step spans and correlation follow the
[runtime spans](../node-rpc-envelope.md#runtime-spans). The update's spans carry the operation, the node
type, both exact births and the revisions: the "primary updating" and "primary updated" announcements,
the record's write, the successor's install, and every refusal by its typed reason.
