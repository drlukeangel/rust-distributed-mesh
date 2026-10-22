# Node primary update

[Lifecycle index](../fabric-mesh-node-lifecycles.md) · [States](states.md) · [Op contract](../node-rpc-envelope.md#primaryupdate-op-0x21-rdm) · [Elections](../fabric-node-lifecycle-elections.md)

`node.primary.update` moves the primary of any (mesh, node_type) cohort, node-admins and every other
type alike, and completes as `node.primary.updated`. `node.fabric-primary.update` moves the
fabric-primary seat by the same primitive and completes as `node.fabric-primary.updated`. Commands only
go down: the fabric-primary commands a mesh-primary, and a mesh-primary commands the primaries of its
mesh's other cohorts. No node of a cohort commands its primary. A stop, delete, restart, replace or
`node.connections.delete { scope: All }` of a primary runs it first.

## Operation contract

| Field | Contract |
|---|---|
| Commander | Above the holder: the fabric-primary for a mesh-primary, the mesh-primary for every other cohort of its mesh. The mesh-primary owns its nodes always: a command the fabric-primary originates for one of them reaches it as a forwarded command through the mesh-primary, never directly |
| Writer | The cohort's seat row is written by the mesh's one row writer, the mesh-primary (R-OW1, R14), never by the successor, who is not the writer until that row exists |
| Holder | Makes the `node.primary.update` call on its successor's exact birth |
| Roles | For the `node-admin` cohort the holder is the mesh-primary, so it is also the writer, and the commander is the fabric-primary. For any other cohort the mesh-primary is commander and writer, and the holder only makes the call |
| Commit | The one seat row at the next revision(s), naming the successor. Before the row nothing has changed; after it the seat is the successor's and the ordinary rules apply. There is no path back |
| Both seats | When the holder holds the mesh seat and the fabric seat and the mesh-seat successor is in the same mesh, the one row commits both at their next revisions |
| Precondition | Before the initial fabric first reaches ready-for-traffic, every call is refused `fabric-not-ready` ([States](states.md#fabric-not-ready)) |
| Completion | `node.primary.updated { revisions }`, answered by the successor on the holder's call; `node.fabric-primary.updated` for the fabric seat |
| Wire | [PrimaryUpdate / PrimaryUpdated on 0x21](../node-rpc-envelope.md#primaryupdate-op-0x21-rdm) |

## Numbered end-to-end sequence

Holder NA1, successor NA2 for the `node-admin` cohort. For another cohort read "the mesh-primary" for
every writer step and B1/B2 for holder and successor; for a non-admin cohort the order of steps 1 and 5
swaps: the mesh-primary is writer and commander, so it pre-checks, announces and writes the row first
(steps 2-5), and its command to the holder B1 comes after the row and carries it; B1 then makes the call
of step 6. For the `node-admin` cohort the holder is commanded first because it is the writer.

1. The holder is commanded from above, as a step of the Build that stops, deletes, restarts, replaces or
   runs `node.connections.delete { scope: All }` on it.
2. The writer pre-checks from its own view: the cohort election over the holder's cohort with the holder
   excluded names a ReadyForTraffic successor. If it names none, the step is refused by name and nothing
   has changed, no cut-off, no row ([no-successor cases](#no-successor-cases)).
3. The writer, as the mesh-primary and sole author of member status, publishes "primary updating":
   operation, cohort, the holder's and the successor's exact births, the current and next revisions. It
   states intent only; receivers keep the current holder until the committed row names the next revision.
4. For the `node-admin` cohort, NA1 sets its write cut-off and finishes every store write already issued
   (R14). The mesh seat's cut-off covers the mesh's rows (`meshes/<mesh>`, `nodes/<mesh>/`); the fabric
   seat's covers `fabric/` and Build acceptance; when both move, one cut-off covers both. A non-admin holder
   has no mesh rows to finish writing (its own `connections/<node>/` rows are its own, R14).
5. The writer writes the one seat row at the next revision(s) naming the successor. This is the commit.
   From this write the holder is no longer primary.
6. The holder calls `node.primary.update` on the successor's exact birth, carrying the row's content. The
   successor reads the row, installs itself as primary (for the `node-admin` cohort, its mesh's row
   writer from that revision), and answers `node.primary.updated { revisions }` on that call. A non-admin
   holder then answers its commander's command.
7. The writer publishes "primary updated" with the same identifiers. A late "updating" never undoes an
   "updated": the committed revision, per cohort, is these two frames' order key in the one gossip
   receive path (R-RX3); nothing orders inside a handler.
8. Only then does the commanding Build continue, and only once the successor's installation is
   acknowledged. The successor is now the holder's primary (for the `node-admin` cohort, its
   mesh-primary) and commands the holder's connections delete, stop, restart or replace.

## Failure contract

- **Refused before the row** (step 2, or the writer's own pre-check of the call's revision): refused by
  name, nothing changed, the Build's step fails by name. Nothing was cut off, so nothing is restored.
- **The seat-row write fails between the cut-off and the commit** (steps 4-5, `node-admin` cohort). The row
  is a local write, temp file, fsync, rename (R14's fs contract), and it answers a named error or
  succeeds. A named error is a confirmed non-commit: the step fails by name naming the error, and NA1
  lifts its cut-off under the unchanged revision, because a primary that cannot write is a worse state
  than a failed step. The one uncertain case is NA1 dying between the cut-off and the rename; NA1 resolves
  it itself on restart by reading the row (committed: it is no longer primary; absent: it is, under the
  unchanged revision).
- **After the row, the successor is the committed holder whatever the call returns.** The receiver never
  refuses the call for the birth it names (R-UC1a, R-RX5); it installs the committed view from the row and
  answers, and whether it is the holder is a projection of the row against its own birth. A call that
  reached a process other than the named birth, a refusal or no answer is never an exit proof (only the
  provider's exit proof vacates a seat; R-SW1: a failed probe never proves death). The holder's call fails
  by name, the workflow runs node-recovery's tickle ladder on the successor's exact birth and retries the
  call against the resolved birth; the seat vacates only on the provider's exit proof, and then the
  ordinary rule fills it (lowest ReadyForTraffic NodeId of the cohort). No row is rewritten.
- **The new holder's process is gone after the row.** When the provider confirms it, the step fails by name,
  `the new holder's process is gone, provider-confirmed, after the committed row`, and the Build is Failed
  (R-BI4). The ordinary rule then seats whoever it seats, the holder included. A holder the rule seats again
  writes its seat record at the next revision first and announces it second (`Seated`), for each seat the rule
  gives back and no other, and restores only those seats' writes under the new revision. The new holder's node
  goes through [node recovery](node-recovery.md) like any member whose process is gone. A caller's resubmit
  opens a new run, so a new transfer under its own key, which excludes the holder.
- **The commanded arm.** When a node.primary.update the fabric-primary commanded ends, the holder answers it
  with the Build carrier's terminal, `Failed { step, reason }` on op `0x21`; `Incomplete` is never used for a
  step that ended. The fabric-primary fails the step on that variant, never on the reason's text. The reason
  records the step, the new holder's node id and incarnation, the seats the row moved with their revisions,
  who the ordinary rule seated, and the provider's proof that the new holder's process is gone. The
  fabric-primary claims the attempt at the claim door before it commands.
- **Indeterminate after the row:** the row resolves it. An identical retry of the call replays the
  committed result before any revision check; a call with a conflicting payload for the same operation
  is refused by name.
- **The holder lost after the row, before "updated":** the successor and every reader take the seat from
  the row; the next writer to read the row publishes "updated". Completion needs neither gossip delivery
  nor the holder's survival.
- **Discovery is not installation.** A later attempt that reads the committed row has discovered the
  commit, not installed it. The resuming executor installs the committed view locally and obtains the
  successor's `node.primary.updated` (its installation acknowledgement) before it commands the departing
  holder's destructive step. A successor whose process the provider confirms gone is handled above.
- The workflow continues only after the result is installed and acknowledged: `node.primary.updated`
  answered to the resuming executor's call, never a read alone.

<a id="no-successor-cases"></a>

## No-successor cases

- **No other node of the type in the mesh:** `node.primary.update` commanded for a cohort with no other
  node of that type in the mesh is refused by name, `cannot transfer: no secondary of type <node_type> in
  mesh <mesh>`, until a secondary is available. The holder stays primary, nothing is cut off, no row is
  written, and the workflow that commanded it (the stop, restart or delete of that node) fails by name with
  that reason. The one exception: the delete of a mesh's only node skips the update. A mesh's sole
  `broker` cannot be stopped or restarted until a second broker is ReadyForTraffic. No yield row and no
  vacancy inferred from a stop.
- **No other mesh-primary for the fabric seat:** the fabric arm's own refusal, [below](#fabric-seat).
- **The last node-admin of a mesh:** mesh.delete's path, and mesh.delete never updates the mesh primary.
  The mesh-primary keeps its seat and keeps publishing through the whole teardown, and a separate
  node.delete with the fabric-primary as executor deletes it last ([mesh.delete](mesh-delete.md)). If the
  mesh holds the fabric seat, that seat moves first by `node.fabric-primary.update` with the departing mesh
  excluded; nothing else moves.
- **Fabric shutdown:** fabric.delete moves no seat; seats remain until deletion.

<a id="fabric-seat"></a>

## The fabric seat: node.fabric-primary.update

The fabric-primary seat is its own property, projected as `is_fabric_primary` (never a settable field: a
published bit is a projection of the committed result), with its own op, `node.fabric-primary.update`. It
runs the same sequence through the same transfer primitive and completes as `node.fabric-primary.updated`
(never `node.primary.updated`, which is the mesh seat's), the holder (a mesh-primary by definition) as
writer and caller, the fabric-primary's commander being the workflow that stops, restarts, deletes or
runs `node.connections.delete { scope: All }` on it, or mesh.delete. Only the successor search differs.

Successor search for the commanded transfer:

1. the holder's own mesh: the lowest ReadyForTraffic node-admin NodeId there, where the seat stays by
   preference;
2. only when that mesh has none, or it is the departing mesh under mesh.delete: the lowest NodeId among the
   other ReadyForTraffic mesh-primaries;
3. **neither found: refused by name**, `cannot transfer fabric seat: no other node-admin ReadyForTraffic in
   mesh <mesh>, no other mesh-primary ReadyForTraffic`, naming both searches and what each found. The
   holder stays fabric-primary, nothing is cut off, no row is written, and the commanding workflow fails
   with that reason until a successor is available.

Where the refusal bites: a single-mesh fabric with one node-admin cannot stop, restart or delete its
fabric-primary until a second node-admin is ReadyForTraffic; mesh.delete of the only mesh, or of the
fabric-primary's mesh when no other mesh has a Ready primary, is refused the same way, and deleting the
last mesh is fabric.delete's path.

The ordinary election is untouched by this search: on a proven loss the seat waits while only Pending
admins of the mesh survive and leaves the mesh only when every node-admin birth of the mesh is proven gone
or has left. The search above is for the commanded transfer only.

**Where the row lives.** The one committed row is the fabric's next seat move, `seat-moves/<T>--<node.id>--<incarnation>` of its
writer on the fabric's shared store root (`seat-move/1`: each seat it moves with its revision before and
after, the operation, and the exact birth that wrote it), written at the next `T` once the writer has
folded every earlier move, whether one seat or both move. Every reader folds the rows in key order and
ignores a row that does not follow the fold (Q109). The row writer writes it: the holder for the `node-admin`
cohort, the mesh-primary for every other. An outside successor of the fabric seat reads it there, and the
leadership view is folded from the seat moves. After the row the writer seals the Build lane of each seat
it moved at the revision it moved from (`lane-seal/1`); the successor appends to that lane only once the
seal stands, and writes the seal itself when the writer was lost before it.

**Both seats on one holder:** when the mesh-seat successor is in the same mesh (search step 1), the one row
commits both seats to the successor at their next revisions, which makes it mesh-primary and
fabric-primary at the same boundary. Under mesh.delete the mesh seat does not move at all: the row commits
the fabric seat to the outside mesh-primary and the incumbent stays mesh-primary until its own node.delete.

## Command, completion and gossip messages

```text
commander → holder        (a step of the Build that stops, deletes, restarts, replaces or deletes the holder's connections)
writer                    pre-check: a ReadyForTraffic successor in the cohort, the holder excluded
writer → mesh channel, backbone   "primary updating" (operation, cohort, both exact births, revisions)
writer                    write cut-off, issued writes finished (node-admin cohort); the one seat row at the next revision(s)
holder → successor        node.primary.update (PrimaryUpdate, carrying the row's content)
  ← node.primary.updated { revisions }   (PrimaryUpdated)
writer → mesh channel, backbone   "primary updated" (same identifiers)
commander's Build continues under the successor
```

"Primary updating" and "primary updated" are announcements, never authority: the committed seat row is
the authority. They are best-effort dissemination, and a stop does not wait for their delivery (R-NS2):
the executing authority publishes a committed fact as its author.

## Mermaid sequence diagram

```mermaid
sequenceDiagram
    participant FP as Fabric-primary (commander)
    participant NA1 as NA1 (holder and writer)
    participant NA2 as NA2 (successor)
    participant G as Mesh channel and backbone
    FP->>NA1: Build step on NA1 (stop, delete, restart, replace, connections delete)
    Note over NA1: pre-check: a ReadyForTraffic successor, NA1 excluded
    NA1->>G: primary updating
    Note over NA1: write cut-off, issued writes finished, the one seat row (commit)
    NA1->>NA2: node.primary.update (row content)
    Note over NA2: install from the row
    NA2-->>NA1: node.primary.updated { revisions }
    NA1->>G: primary updated
    Note over FP,NA2: the Build continues; NA2 commands NA1's destructive step
```

## Observability contract

Trace propagation, step spans and correlation follow the
[runtime spans](../node-rpc-envelope.md#runtime-spans). The update's spans carry the operation, the
cohort, both exact births and the revisions: the writer's "primary updating" and "primary updated"
announcements, the seat row's write, the successor's install, and every refusal by its typed reason.
