# Node primary update

[Lifecycle index](../fabric-mesh-node-lifecycles.md) · [States](states.md) · [Op contract](../node-rpc-envelope.md#primaryupdate-op-0x21-rdm) · [Elections](../fabric-node-lifecycle-elections.md)

`node.primary.update` moves the primary of the nodes of one type in one mesh, node-admins and every
other type alike, and completes as `node.primary.updated`. `node.fabric-primary.update` moves the
fabric-primary by the same primitive and completes as `node.fabric-primary.updated`. Commands only go
down: the fabric-primary commands a mesh-primary, and a mesh-primary commands the primaries of its
mesh's other node types. No node commands its own primary. A stop, delete, replace or
`node.connection.delete { scope: All }` of a primary runs it first.

Who is primary is the [election](../fabric-node-lifecycle-elections.md)'s result, computed by every
node-admin from the facts it holds. Becoming primary and recording it are separate: the successor
installs itself from the election and the mesh-primary announces the change; the mesh-primary, as the
one writer of its mesh's rows ([node-identity-and-state.md §2.7](../node-identity-and-state.md#27-a-mesh-is-an-entity-and-node-admin-owns-it)),
records it as `is_primary` on the node's row, and as `is_fabric_primary` on a node-admin's own row for the
fabric-primary. **The write records the role; it does not grant it.** The mesh-primary and the
fabric-primary move together and are never separated. A fabric-primary unheard past the staleness floor
is not succeeded: [fabric recovery](fabric-recovery.md) re-creates it at its path.name (Q122). Under
mesh.delete the fabric-primary hands over to another mesh first
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
| Precondition | None on the fabric's state (Q171) |
| Completion | `node.primary.updated { revisions }`, answered by the successor on the primary's call; `node.fabric-primary.updated` for the fabric-primary |
| Wire | [PrimaryUpdate / PrimaryUpdated on 0x21](../node-rpc-envelope.md#primaryupdate-op-0x21-rdm) |

## Numbered end-to-end sequence

Mesh-primary NA1, successor NA2 for the `node-admin` type. For another type read B1/B2 for the primary
and its successor and "the mesh-primary" for the commander, the announcer and the writer: the order is
the same, command, then the successor becomes primary and announces, then the record. Becoming primary
and recording it are separate for every type.

1. The primary is commanded from above, as a step of the Build that stops, deletes, replaces or
   runs `node.connection.delete { scope: All }` on it.
2. The writer pre-checks from its own view: the election over the nodes of that type in the mesh with the
   primary excluded names a Ready successor. If it names none, the step is refused by name and
   nothing has changed, no row ([no-successor cases](#no-successor-cases)).
3. For the `node-admin` type, NA1 finishes the writes it has issued before it calls NA2 (Q109): once
   NA2 is mesh-primary NA1 writes the mesh's rows no more. A non-admin primary has no mesh rows to finish writing
   (its own connection facts in `connections.jsonl` are its own, R14).
4. The primary calls `node.primary.update` on the successor's exact birth: for a type other than `node-admin`,
   the birth the mesh-primary's command names (step 2). The successor becomes
   primary: it installs the election's result the call carries in its own topology view, answers
   `node.primary.updated { revisions }` on that call, and its next `Digest` carries the change
   (`is_primary`, and `is_fabric_primary` when both moved, at the next revision), its normal
   announcement and the only one ([gossip.md §3.1](../gossip.md#31-frame-catalog)). A non-admin primary
   then answers its commander's command. For the `node-admin` type NA2 is the mesh's writer from this
   moment.
5. The mesh-primary records the change it hears: `is_primary` (and `is_fabric_primary`) at the next
   revision on the successor's row, and its next `MeshTopology` projection carries it, its normal publication.
   For the `node-admin` type that is NA2 recording its own row; for every other type the mesh-primary
   records the successor's row from the successor's digest, the way it records every member fact it
   hears. The write records the role; it does not grant it. No frame of its own announces a primary
   change.
6. The commanding Build continues on the successor's direct acknowledgement of step 4, never on
   gossip: step 5 is the recording of a change that has already happened, and the Build waits for
   neither the digest nor the record. The successor is now the old primary's primary (for the `node-admin`
   type, its mesh-primary) and commands the old primary's connections delete, stop or replace.

## Failure contract

- **Refused before the row** (step 2, or the writer's own pre-check of the call's revision): refused by
  name, nothing changed, the Build's step fails by name.
- **The record's write fails** (step 5). The row is a local write, temp file, fsync, rename (R14's fs
  contract), and it answers a named error or succeeds. The step completed on the acknowledgement of
  step 4 and the role is the election's, not the write's: a named error is handled as every storage
  error is, named on its span and surfaced, it fails nothing already completed, and the successor stays
  what the election names it. The row is owed by its writer: the mesh-primary retries the write of the
  row's current state under the one backoff rule ([Node RPC §43](../node-rpc.md#43-owed-messages-delivery-is-the-runtimes))
  until the store acknowledges it, on its own clock, never waiting for a digest, a birth or a primary
  change, and marks the row written only on that acknowledgement, while it is the writer. A write that
  never succeeds is surfaced as a stalled row; it blocks and reverses nothing. A writer dying before the rename changes nothing:
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
  lowest Ready node of that type in the mesh is primary, its digest carries it and the
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
- **The old primary lost after the update:** the successor and every reader take the primary from the
  election; the successor's digest carries its role, and the mesh-primary (the successor itself for the
  `node-admin` type) records it. Completion needs neither gossip delivery nor the old primary's
  survival.
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
  workflow that commanded it (the stop or delete of that node) fails by name with that reason.
  The exceptions that skip the update: the delete of a mesh's only node, and the delete of a type's only
  node in its mesh, whether the node leaves the topology (a removal) or is re-created at the same
  `path.name` (node recovery, an executable replacement); the type has no primary until the election names
  the new birth Ready. A mesh's sole `broker` cannot be stopped until a second
  broker is Ready. No row and no vacancy inferred from a stop.
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
deletes or runs `node.connection.delete { scope: All }` on it, or mesh.delete. Only the
successor search and the row differ.

Successor search for the commanded transfer:

1. the fabric-primary's own mesh: the lowest Ready node-admin NodeId there, where the
   fabric-primary stays by preference;
2. only when that mesh has none, or it is the departing mesh under mesh.delete: the lowest NodeId among
   the other Ready mesh-primaries;
3. **neither found: refused by name**, `cannot transfer fabric-primary: no other node-admin
   Ready in mesh <mesh>, no other mesh-primary Ready`, naming both searches and what
   each found. The fabric-primary stays fabric-primary, no row is written, and the commanding workflow
   fails with that reason until a successor is available.

Where the refusal bites: a single-mesh fabric with one node-admin cannot stop or delete its
fabric-primary until a second node-admin is Ready; mesh.delete of the only mesh, or of the
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
commander → primary       (a step of the Build that stops, deletes, replaces or deletes the primary's connections)
writer                    pre-check: a Ready successor of the type in the mesh, the primary excluded
primary                   issued writes finished (node-admin type)
primary → successor       node.primary.update (PrimaryUpdate)
successor                 becomes primary: installs the election's result in its topology view
  ← node.primary.updated { revisions }   (PrimaryUpdated)
commander's Build continues under the successor
successor → mesh channel  its next Digest carries is_primary (its normal announcement)
mesh-primary              records is_primary at the next revision on the successor's row from what it hears (NA2 its own row); its next MeshTopology projection carries it
```

No gossip frame announces a primary change: the successor's `Digest` and the mesh-primary's `MeshTopology`
projection are the normal announcement and publication, best-effort dissemination a stop does not wait
for (R-NS2). The election's result is the fact, and the row records it.

## Mermaid sequence diagram

```mermaid
sequenceDiagram
    participant FP as Fabric-primary (commander)
    participant NA1 as NA1 (mesh-primary)
    participant NA2 as NA2 (successor, writer)
    participant G as Mesh channel and backbone
    FP->>NA1: Build step on NA1 (stop, delete, replace, connections delete)
    Note over NA1: pre-check: a Ready successor, NA1 excluded
    Note over NA1: issued writes finished
    NA1->>NA2: node.primary.update
    Note over NA2: becomes primary: installs the election's result
    NA2-->>NA1: node.primary.updated { revisions }
    Note over FP,NA2: the Build continues; NA2 commands NA1's destructive step
    NA2->>G: Digest (is_primary, next revision)
    Note over NA2: records is_primary on its own row; MeshTopology projection carries it
```

## Observability contract

Trace propagation, step spans and correlation follow the
[runtime spans](../node-rpc-envelope.md#runtime-spans). The update's spans carry the operation, the node
type, both exact births and the revisions: the successor's install, the record's write, and every
refusal by its typed reason.
