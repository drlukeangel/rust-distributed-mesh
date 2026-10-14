# Proposal: node.type, leadership at the node.type level, node.primary.update and node.fabric-primary.update

This proposal turns three of Luke's 2026-10-12 directions into exact doc text and code work:

- a node's type as its own field;
- one election per (mesh, node.type) cohort;
- one primitive that moves a primary, `node.primary.update`, with its fabric twin, `node.fabric-primary.update`.

Architecture doc edits land only on Luke's yes to this proposal; that yes is the yes to each edit below. The code items are conformance to rulings already given and are carried by the PRD blocks named in Part E. Every file:line is evidence on rafka-v2 main and RDM main `a35ad7f6`; dev re-pins at landing.

## Docs of record

- Rulings page `docs/plans/rulings/i143-rulings-2026-10-12-rx-authz.md`:
  - **R-MT1:** node.primary.update and node.fabric-primary.update.
  - **The node direction:** a path is free text; path and name are two fields; node_type is the node's own field, set by the caller, immutable.
- Luke 2026-10-12: "a node-admin in the mesh shouldn't issue a command to the mesh-primary"; "only a FP should issue commands to a MP"; "NA1 > node.primary.update > NA2 ---> NA2.update(is_primary = true) > node.primary.updated --> NA1 gossips MP updated NA2 ... same for NA as it is for RPC node"; "it's a separate property, same logic, just node.fabric-primary.update"; "should it be Node-Admin = Node-Admin, if blank it defaults to node?"
- Replaced by R-MT1: R-EL1a (the executor N sends TakeSeats to the holder P).
- Unchanged and binding:
  - R14 (writer ladder, one store root per fabric);
  - R-OW1 (the mesh-primary writes its mesh's rows);
  - R-EL1 (a seat moves only by explicit yield or proven loss);
  - R-PB1 (no seat moves before the initial fabric is ready-for-traffic);
  - R-RX1 (no receiver checks who sent a call).
- Pages: fabric-node-lifecycle-elections.md §4 to §6, lifecycles/node-stop.md, lifecycles/node-connections-delete.md, lifecycles/states.md "Leadership", node-rpc-envelope.md (Meta field groups; TakeSeats), gossip.md (SeatedTogether), node-identity-and-state.md §2.

## Part A — node.type

### A1. The field

- `node_type` is a field of the node record, a string. The caller sets it in node.create's spec; nothing derives it from the path, the name or the binary.
- **Blank means `node` (Q6, Luke 2026-10-12: "empty node_type = node").** An omitted or empty value is stored as `node` before anything groups on it: a plain node with no product role, RDM's `RpcNode` today. Every node has a cohort and the election never groups on an empty string.
- **`node-admin` is the one value RDM gives meaning to:** a mesh-primary comes only from a mesh's `node-admin` cohort, and the fabric-primary only from mesh-primaries (Part B).
- **Every other value is the product's own string.** Rafka's are `broker`, `gateway`, `compute` (`crates/rafka-entity-topology/src/record.rs:500-506`). RDM groups on the string and attaches no other meaning to it.
- **Grammar:** the tag-label grammar (R-TG2a), 1-64 characters of `a-z 0-9 - _ . ~`, no leading `.`. A value outside it is refused by name at node.create. Leading and trailing whitespace is refused, not trimmed.
- **Immutable identity, not meta:** node.update refuses a change by name. On replace and node recovery the successor's create spec names the same `node_type`, the way it names `path` and `mesh`; nothing is copied from the predecessor's record and it is never in the caller-owned group.

**Evidence for the change:**
- RDM derives the type from the name's middle segment. `PathName { mesh, kind: NodeKind, ordinal, old, new }` (`rafka-mesh-entity/src/path.rs:71`) parses `admin` to `NodeAdmin` and `rpc` to `RpcNode` (`path.rs:45-57`).
- `NodeKind` carries Rafka's product names `Broker`, `Gateway`, `Compute` (`path.rs:12-22`). build.md's ownership test puts those in Rafka.
- Rafka already has the field: `NodeDigest.node_type: String` (`record.rs:21`).

### A2. Doc edits

1. **node-identity-and-state.md :65** (`path.name` row)
   - Now: "path `<mesh>.<type>`, name the ordinal — `mesh1.broker.3`".
   - Becomes: "path and name are the node's two fields, joined only as the path.name index key; both are free text the caller chooses at node.create (`<mesh>.<kind>.<name>` by convention, a name, never an ordinal); the node's type is its own field, `node_type` (§2), never read from the path".
2. **node-identity-and-state.md :87-89**
   - Now: "the path is `<mesh>.<type>` ... and the name is the node's ordinal. A single node is `mesh1.broker.1` ...".
   - Becomes: "the path and the name are free text set at node.create; nothing is derived from either. A single node is created, for example, as `mesh1.broker.north`".
3. **node-rpc-envelope.md node properties:** `node_type` is in the node properties table (`path`, `mesh`, `node_type`: set by the create spec, immutable for the node's life), per node-properties-own-commands.md §1.
4. **The node entity in node-identity-and-state.md §2:** add `node_type` to the node's fields with A1's grammar, default and immutability.

## Part B — leadership at the node.type level

### B1. The rule

- Every (mesh, node_type) cohort elects its primary by the one rule of elections §4.1 and §5: a living holder keeps its seat; a vacancy goes to the lowest ReadyForTraffic NodeId of the cohort.
- A node's `is_primary` means it is the primary of its own type's cohort in its mesh.
- The mesh-primary is the primary of the mesh's `node-admin` cohort (§4.2).
- The fabric-primary is its own property, `is_fabric_primary`, elected from the mesh-primaries (§4.3) and kept in its holder's mesh (§4.3, §6).
- Election reads `node_type`, never the name.

**Evidence:** today the election groups on the parsed kind: `Topology::cohort` filters `n.mesh == mesh && n.kind == kind` (`rafka-node-admin-core/src/topology.rs:145`), and `cohort_primary` at `:182` picks the holder.

### B2. Doc edits

1. **elections §4.1**
   - Now: "For every (mesh, node kind) cohort, the one imported resolver applies the same eligibility/key rule."
   - Becomes: "For every (mesh, node_type) cohort, the one imported resolver applies the same eligibility/key rule. A node's type is its `node_type` field ([node identity](node-identity-and-state.md)), never its name. Each cohort's primary is that node's `is_primary`."
2. **elections §4.2**
   - Now: "(mesh, node-admin) cohort winner == mesh primary".
   - Becomes: "(mesh, `node-admin`) cohort winner == mesh primary".
   - No other change.

## Part C — node.primary.update

### C1. The primitive

One primitive moves the primary of any (mesh, node_type) cohort: node-admins and every other type alike. Commands only go down. No node of a cohort commands its primary.

- The fabric-primary commands a mesh-primary.
- A mesh-primary commands the primaries of its mesh's other cohorts (Q3).

**One writer, one row, one point of no return.** The seat row is the commit (elections §4.4: the committed seat record is authoritative resolver input). It is written by the mesh's one row writer (R-OW1, R14), never by the successor, who is not the writer until that row exists. Three roles, which coincide for the `node-admin` cohort and separate for every other:

- the **commander**, above the holder: the fabric-primary for a mesh-primary, the mesh-primary for every other cohort of its mesh (Q3). The mesh-primary is the owner of its nodes, always: a command the fabric-primary originates for one of them reaches it as a proxy command through the mesh-primary, never directly;
- the **writer** of the cohort's seat row: the mesh-primary of the mesh;
- the **holder**, who makes the `node.primary.update` call on its successor (Luke: "NA1 calls node.primary.update on NA2 ... same for an RPC node").

For the `node-admin` cohort the holder NA1 is the mesh-primary, so it is also the writer, and the commander is the fabric-primary. For a `broker` cohort the mesh-primary is commander and writer, and the holder B1 only makes the call. When NA1 holds both seats, the one row commits both at their next revisions (Q1). Before the row nothing has changed; after it the seat is the successor's and the ordinary rules apply. There is no path back.

**Sequence** (NA1/NA2 for the `node-admin` cohort; for another cohort read "the mesh-primary" for every writer step and "B1/B2" for holder and successor). For a non-admin cohort the order of steps 1 and 5 swaps: the mesh-primary is writer and commander, so it pre-checks, announces and writes the row first (steps 2-5), and its command to the holder B1 comes after the row and carries it; B1 then makes the call of step 6. For the `node-admin` cohort the holder is commanded first because it is the writer:

1. The holder is commanded from above, as a step of the Build that stops, deletes, restarts, replaces or runs `node.connections.delete { scope: All }` on it.
2. The writer pre-checks from its own view: the cohort election over the holder's cohort with the holder excluded names a ReadyForTraffic successor. If it names none, the step is refused by name and nothing has changed, no cut-off, no row (the no-successor cases below).
3. The writer, as the mesh-primary and sole author of member status, publishes "primary updating": operation, cohort, the holder's and the successor's exact births, the current and next revisions. It states intent only; receivers keep the current holder until the committed row names the next revision.
4. For the `node-admin` cohort, NA1 sets its write cut-off and drains every store write already issued (R14). The mesh seat's cut-off covers the mesh's rows (`meshes/<mesh>`, `nodes/<mesh>/`); the fabric seat's covers `fabric/` and Build acceptance; when both move, one drain covers both. A non-admin holder has no mesh rows to drain (its own `connections/<node>/` rows are its own, R14).
5. The writer writes the one seat row at the next revision(s) naming the successor. This is the commit. From this write the holder is no longer primary.
6. The holder calls `node.primary.update` on the successor's exact birth, carrying the row's content. The successor reads the row, installs itself as primary (for the `node-admin` cohort, its mesh's row writer from that revision), and answers `node.primary.updated { revisions }` on that call. A non-admin holder then answers its commander's command.
7. The writer publishes "primary updated" with the same identifiers. A late "updating" never undoes an "updated": the committed revision, per cohort, is these two frames' order key in the one gossip receive path (R-RX3), already in the body so no new field; nothing orders inside a handler.
8. Only then does the commanding Build continue, and only once the successor's installation is acknowledged (below). The successor is now the holder's primary (for the `node-admin` cohort, its mesh-primary) and commands the holder's connections delete, stop, restart or replace.

**Failure contract (every branch):**

- **Refused before the row** (step 2, or the writer's own pre-check of the call's revision): refused by name, nothing changed, the Build's step fails by name. Nothing was cut off, so nothing is restored.
- **The seat-row write fails between the drain and the commit** (steps 4-5, `node-admin` cohort). The row is a local write, temp file, fsync, rename (R14's fs contract), and it answers a named error or succeeds. A named error is a confirmed non-commit: the step fails by name naming the error, and NA1 lifts its cut-off under the unchanged revision, because a primary that cannot write is a worse state than a failed step. The one uncertain case is NA1 dying between the drain and the rename; NA1 resolves it itself on restart by reading the row (committed: it is no longer primary; absent: it is, under the unchanged revision). Two cells.
- **After the row, the successor is the committed holder whatever the call returns.** The receiver never refuses the call for the birth it names (R-UC1a, R-RX5: no receiver decides whether a call was meant for it); it installs the committed view from the row and answers, and whether it is the holder is a projection of the row against its own birth. A call that reached a process other than the named birth, a refusal or no answer is never an exit proof (elections §4.1: only the provider's exit proof vacates a seat; R-SW1: a failed probe never proves death). The holder's call fails by name, the workflow runs node-recovery's tickle ladder on the successor's exact birth and retries the call against the resolved birth; the seat vacates only on the provider's exit proof, and then the ordinary rule fills it (lowest ReadyForTraffic NodeId of the cohort). No row is rewritten.
- **Indeterminate after the row:** the row resolves it. An identical retry of the call replays the committed result before any revision check; a call with a conflicting payload for the same operation is refused by name.
- **The holder lost after the row, before "updated":** the successor and every reader take the seat from the row; the next writer to read the row publishes "updated". Completion needs neither gossip delivery nor the holder's survival.
- **Discovery is not installation.** A later attempt that reads the committed row has discovered the commit, not installed it. The resuming executor installs the committed view locally and obtains the successor's `node.primary.updated` (its installation acknowledgement) before it commands the departing holder's destructive step. A successor whose birth is proven exited is a proven loss, handled above.
- The workflow continues only after the result is installed and acknowledged: `node.primary.updated` answered to the resuming executor's call, never a read alone.

**The no-successor cases**, each named:

- **No other node of the type in the mesh (ruled, Luke 2026-10-12; Q7):** `node.primary.update` commanded for a cohort with no other node of that type in the mesh is refused by name, `cannot transfer: no secondary of type <node_type> in mesh <mesh>`, until a secondary is available. The holder stays primary, nothing is cut off, no row is written, and the workflow that commanded it (the stop, restart or delete of that node) fails by name with that reason. The one exception is R-MT1's: the delete of a mesh's only node skips the update. Consequence stated on the page: a mesh's sole `broker` cannot be stopped or restarted until a second broker is ReadyForTraffic. No yield row and no vacancy inferred from a stop (R-EL1).
- **No other mesh-primary for the fabric seat (Luke 2026-10-12):** the fabric arm's own refusal, C2.
- **The last node-admin of a mesh:** mesh.delete's path, and mesh.delete never updates the mesh primary. The mesh-primary keeps its seat and keeps publishing through the whole teardown, and a separate node.delete with the fabric-primary as executor deletes it last (mesh-delete.md rows 21 and 25, step 7; R-NS1, R-MD1). If the mesh holds the fabric seat, that seat moves first by `node.fabric-primary.update` with the departing mesh excluded (mesh-delete.md step 1); nothing else moves.
- **Fabric shutdown:** fabric.delete moves no seat (elections §6); seats remain until deletion.

Before the initial fabric first reaches ready-for-traffic, every call is refused `fabric-not-ready` (R-PB1).

### C2. node.fabric-primary.update

The fabric-primary seat is its own property, projected as `is_fabric_primary` (never a settable field: elections §1, a published bit is a projection of the committed result), with its own op, `node.fabric-primary.update`. It runs the same sequence through the same transfer primitive and completes as `node.fabric-primary.updated` (never `node.primary.updated`, which is the mesh seat's), the holder (a mesh-primary by definition) as writer and caller, the fabric-primary's commander being the workflow that stops, restarts, deletes or drains it, or mesh.delete. Only the successor search differs.

**Successor search for the commanded transfer:**

1. the holder's own mesh: the lowest ReadyForTraffic node-admin NodeId there, where the seat stays by preference (§4.3);
2. only when that mesh has none, or it is the departing mesh under mesh.delete: the lowest NodeId among the other ReadyForTraffic mesh-primaries;
3. **neither found: refused by name**, `cannot transfer fabric seat: no other node-admin ReadyForTraffic in mesh <mesh>, no other mesh-primary ReadyForTraffic`, naming both searches and what each found. The holder stays fabric-primary, nothing is cut off, no row is written, and the commanding workflow fails with that reason until a successor is available (Luke 2026-10-12).

Where the refusal bites, stated on the page: a single-mesh fabric with one node-admin cannot stop, restart or delete its fabric-primary until a second node-admin is ReadyForTraffic; mesh.delete of the only mesh, or of the fabric-primary's mesh when no other mesh has a Ready primary, is refused the same way, and deleting the last mesh is fabric.delete's path.

The ordinary election is untouched by this search: on a proven loss the seat waits while only Pending admins of the mesh survive and leaves the mesh only when every node-admin birth of the mesh is proven gone or has left (§4.3 :114). The search above is for the commanded transfer only.

**Where the row lives.** The one committed row is the holder's transfer row under its own mesh on the fabric's shared store root, `meshes/<holder mesh>/seat-transfers/<operation>` (today's `SeatTransferRow`), which the holder writes as that mesh's writer whether one seat or both move; an outside successor of the fabric seat reads it there. The per-seat rows under `fabric/seats` go: only the fabric-primary may write `fabric/`, a mesh-seat transfer cannot, and two durable records of one seat is the two-writers shape; the leadership view is folded from the transfer rows.

**Both seats on one holder (Q1):** when the mesh-seat successor is in the same mesh (search step 1), the one row of C1 commits both seats to NA2 at their next revisions, which makes NA2 mesh-primary and fabric-primary at the same boundary. Under mesh.delete the mesh seat does not move at all (the no-successor cases above): the row commits the fabric seat to the outside mesh-primary and the incumbent stays mesh-primary until its own node.delete.

**One primitive, one page.** `fabric.primary.update` (0x21 TakeFabricPrimary / FabricPrimaryTaken, lifecycles/fabric-primary-update.md, where the successor commits the transfer) is the op this primitive replaces: it goes (Part D), its content becomes the fabric arm of the primitive's page (C3), and mesh-delete.md, the workflow page, calls `node.fabric-primary.update` in its place.

### C3. Doc edits

1. **New lifecycle page `lifecycles/node-primary-update.md`, one page for the one primitive**, covering both ops: the mesh seat and, in its own section, the fabric seat's successor search, refusal and the dual-seat row (C2). The failure contract, roles, announcements and cells are stated once.
   - Shape: today's fabric-primary-update.md structure (contract table, numbered sequence, command/completion/gossip messages, observability), which this page absorbs.
   - Index: two rows in fabric-mesh-node-lifecycles.md, `node.primary.update` and `node.fabric-primary.update`, both pointing at this page; the `fabric.primary.update` row goes.
   - `lifecycles/fabric-primary-update.md` is deleted (its op is replaced, Part D); every link to it moves to the new page (mesh-delete.md :18 and :34-35, node-rpc-envelope.md :251, elections §4.4, states.md "Leadership", the lifecycle index).
2. **lifecycles/mesh-delete.md :18 (Seat row) and :34-35 (step 1)**
   - Now: "If the mesh holds the fabric-primary seat, [fabric.primary.update](fabric-primary-update.md) runs first"; "`fabric.primary.updated` confirms the committed transfer before any member is deleted".
   - Becomes: "If the mesh holds the fabric-primary seat, [node.fabric-primary.update](node-primary-update.md) runs first with this mesh excluded from the successor search"; "`node.fabric-primary.updated` confirms the committed transfer before any member is deleted". Rows 21, 25 and step 7 are unchanged: the mesh-primary keeps its seat to the end and never runs node.primary.update under mesh.delete.
2. **lifecycles/node-stop.md :17** (Seat holder row)
   - Now: "A target holding a seat is stopped by its same-mesh successor; SeatTransfer (TakeSeats, SeatsCommitted installed) precedes `node.connections.delete`".
   - Becomes: "A target holding a seat runs [node.primary.update](node-primary-update.md) first, commanded from above; its successor, now its primary, then commands the stop".
3. **lifecycles/node-connections-delete.md step 0 and :29**
   - Now: "the Build's executor is its same-mesh successor. It runs the TakeSeats seat transfer to the holder and continues only once it has installed SeatsCommitted".
   - Becomes: "the holder first runs node.primary.update (and node.fabric-primary.update when it holds the fabric seat), commanded from above; the Build continues once node.primary.updated is answered, under the new primary".
   - Row :29 says the same.
4. **lifecycles/states.md "Leadership"**
   - Now: "requires the committed seat transfer described in node.connections.delete and fabric.primary.update".
   - Becomes: "requires [node.primary.update](node-primary-update.md) first (node.fabric-primary.update for the fabric seat)".
5. **node-rpc-envelope.md "TakeSeats: Build-driven seat transfer" (:1449-1480)**
   - Now: `N → P TakeSeats { ... }`, `P → N SeatsCommitted { seats }`.
   - Becomes: `holder → successor PrimaryUpdate { seats: [(cohort, holder: { node_id, incarnation }, successor: { node_id, incarnation }, revision)], build_id, attempt, operation }` carrying the committed row's content (one or two seats), exact births on both sides and never a name, and, on that call's reply stream, `PrimaryUpdated { revisions }`. Its refusals:
     - RejectedConflictingPayload (same operation, different content: the row decides);
     - `fabric-not-ready`.
   - An identical repeat replays the committed result. There is no RejectedWrongBirth (R-UC1a, R-RX5: a receiver never refuses a call for the node id it names), no RejectedStaleSeat and no RejectedNotEligible on the receiver: eligibility is the writer's pre-check before the row, and the row is already committed when the call is made.
   - The fabric seat is a second entry in `seats`, cohort `fabric`.
   - The block keeps TakeSeats' idempotency and retry text verbatim under the new names (identical call replays the row; conflicting payload refused; the row resolves uncertainty).
   - It is a wire change (durable format moved).
6. **gossip.md :158 and :276-292** (SeatedTogether)
   - Now: "the seat holder, from its committed seat row, before it answers SeatsCommitted".
   - Becomes: two announcements on the mesh channel and the backbone, "primary updating" before the row and "primary updated" after `node.primary.updated`, each naming operation, cohort, both exact births and the revisions. The announcer is the mesh-primary (NA1 itself for the `node-admin` cohort), the sole author of member status; a node of another cohort never publishes its own seat.
   - Peers carry them as they carry SeatedTogether today: announcements, never authority. The committed seat row is the authority; a late "updating" never undoes an "updated".
7. **fabric-node-lifecycle-elections.md §4.4, second paragraph** ("A Build that runs `node.connections.delete { scope: All }` on, stops, restarts or replaces a seat holder … transfers the holder's seats by TakeSeats … Its executor is the same-mesh successor"): becomes the C1 sequence by reference, the holder writing the one committed row, then node.primary.update; the executor line goes with R-EL1a.
8. **build.md §12.4 :543-545** ("A delete of the node-admin that holds mesh2's seat runs with the same-mesh successor as executor, which first takes the seats by TakeSeats"): becomes "the holder first runs node.primary.update, commanded by the fabric-primary; its successor, now mesh2's primary, executes the delete". The build proposal's A4 cites TakeSeats and is corrected with it.
9. **Code comments that cite TakeSeats** (`fabric_storage.rs` `SeatTransferRow`, `executor.rs` `seated_once_gone`): the same sweep, in the landing.

## Part D — what goes

- R-EL1a's direction: the executor N sending TakeSeats to the holder P. That covers `executor.rs` `seated_once_gone` as the executor of a seat holder's stop, the SeatTransfer step, and the TakeSeats/SeatsCommitted call.
- `fabric.primary.update` as an op of its own: the 0x21 `TakeFabricPrimary` / `FabricPrimaryTaken` frames where the successor commits the transfer, and `lifecycles/fabric-primary-update.md`. 0x21 carries the primitive's `PrimaryUpdate` / `PrimaryUpdated` for both seats; the incumbent writes the row. A wire change on 0x21, `durable format moved`, with its flame.
- `NodeKind::Broker`, `Gateway`, `Compute`, and the kind and ordinal inside `PathName`.

## Part E — code (PRD blocks)

- **e12.s41:** `node_type` (Part A), with `PathName`'s kind and ordinal replaced. This is a durable format move.
- **e12.s23, the R-MT1 item:** node.primary.update and node.fabric-primary.update (Parts B and C). A seat is building it now on `i143-s23-primary-update`.
  - The flame comes from a live mesh: a mesh-primary stop commanded by the fabric-primary, with both gossips, `node.primary.updated`, and every node, the fabric-primary included, naming NA2.
  - It fixes the e12.s56 gate red `node_replace` `deleting_a_parked_predecessor...`.
  - **Cells, beyond the happy path** (each asserts the committed row and who every node names as primary):
    - refusal before the row: no successor, nothing changed, no cut-off;
    - seat-row write fails after the drain: the step fails naming the error and NA1 writes again under the unchanged revision;
    - NA1 dies between the drain and the rename: on restart it reads the row and is, or is not, primary accordingly;
    - commit then lost reply: identical retry replays the row; a conflicting payload is refused;
    - incumbent lost after the row and before "updated": the seat is the successor's from the row; the workflow's next attempt installs the view, obtains `node.primary.updated`, then continues;
    - reordered gossip: "updating" delivered after "updated" changes nothing;
    - dual-seat transfer observed by another admin: one row, both seats at their next revisions, read by a third admin;
    - the call after the row reaches a process that is not the named successor's birth, the successor alive: no refusal, the successor stays the committed holder, the retry against its resolved birth completes;
    - successor proven exited after the row: the seat vacates on the provider's proof and the ordinary rule fills it;
    - a non-admin cohort: the mesh-primary writes the row and announces, the broker holder makes the call, the broker successor installs;
    - the sole member of a non-admin type stopped: refused `cannot transfer: no secondary of type …`, the holder still primary, no row, the stop failing with that reason; the same stop succeeds once a second member is ReadyForTraffic;
    - the fabric-primary of a single-admin single-mesh fabric stopped: refused naming both searches; succeeds once a second node-admin of its mesh is ReadyForTraffic, both seats moving in one row;
    - mesh.delete of a mesh holding the fabric seat with another Ready mesh-primary: the fabric seat moves outside first, the mesh-primary stays primary and publishing until the fabric-primary's node.delete removes it;
    - mesh.delete of the only mesh: refused with the fabric-seat reason.

## Open questions (each with a recommendation)

- **Q1. One admin holds both seats.**
  - Recommendation: one committed row moving both seats at their next revisions, written by the holder after one drain (C1). Not fabric-first then mesh: the fabric candidates are mesh-primaries and NA2 is not one until the mesh seat moves, and the first transfer's cut-off would block the second's write. The two properties and the two op names stay distinct; their coordinated transfer is one commit.
- **Q2. Gossip.**
  - Recommendation: two new frames, "primary updating" and "primary updated", named for what they say, each carrying operation, cohort, both exact births and the revisions; SeatedTogether is retired. The committed row stays the only authority. This is a gossip change: Luke's yes plus the flame.
- **Q3. Who commands a non-admin cohort's primary update? Ruled A (Luke 2026-10-12, "MP"; "it can come from the FP for some reason, but as a proxy command: the MP is the owner of its nodes, always"):** that mesh's primary; a command the fabric-primary originates for a mesh's node is proxied through the mesh-primary, never sent to the node directly (the `node-admin` cohort's primary). Commands go down: the fabric-primary commands mesh-primaries; a mesh-primary commands its own mesh's nodes. The mesh-primary is also the announcer for those cohorts (sole author of member status).
- **Q4. fabric-primary-update.md hands the seat to "an eligible mesh-primary of another mesh"** (its Intent row, used by mesh.delete); §4.3 keeps it in the holder's mesh. **Settled (Luke 2026-10-12):** mesh.delete is a workflow with its own page, mesh-delete.md, and this is the primitive it calls. fabric.primary.update is the op the primitive replaces, so it and its page go (Part D, C3); mesh-delete.md calls node.fabric-primary.update with the departing mesh excluded (search step 2 of C2) and otherwise changes nothing: the mesh-primary stays primary until the fabric-primary's node.delete removes it.
- **Q5. node_type grammar.**
  - Recommendation: the tag-label grammar (A1), one grammar for both; whitespace refused, never trimmed.
- **Q6. The default for a blank `node_type`. Ruled (Luke 2026-10-12): empty `node_type` = `node`.** An omitted or empty value is stored as `node` before grouping; whitespace is refused (A1).
- **Q7. A transfer with no successor. Ruled (Luke 2026-10-12):** `node.primary.update` for a type with no other node of that type in the mesh is refused by name, `cannot transfer: no secondary of type <node_type> in mesh <mesh>`, until a secondary is available; `node.fabric-primary.update` with no other node-admin ReadyForTraffic in the mesh and no other mesh-primary ReadyForTraffic is refused the same way, naming both searches. The holder stays primary, nothing is cut off, no row is written, and the commanding workflow fails with that reason. R-MT1's one exception stands: the delete of a mesh's only node skips the update. No yield row, no vacancy inferred from a stop (R-EL1). The consequences are stated on the page: a mesh's sole broker, and a single-admin single-mesh fabric's primary, cannot be stopped or restarted until a secondary is ReadyForTraffic.
