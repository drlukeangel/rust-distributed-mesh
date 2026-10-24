## Roll: a running fabric's members onto a new executable set, as one Build

Proposal for i143 (Luke 2026-10-22, arm (a) of the i141 B3 packet, rafka-v2 `36781090eb`: "RDM owns
the roll"). R1-R3 have Luke's yes (2026-10-24, as written). Revised the same day on Architect 1's
review of `2195f9379`: its three gaps are closed in the operation below (step 2, the launch set; step 3,
resume without a plan record; step 6, the one-admin mesh), the R2 sentence is amended with its arm
unchanged, the RDM/Rafka split is written down, and step 5 adds the fabric-primary's own replacement,
which the first text left out. Revised again the same day on Architect 1's review of `578a6044d`: the
set lives on the accepted topology, so every later Build carries it (step 2), and step 5 separates a
node-admin primary from a primary of another type. R4 is ruled (Luke 2026-10-24, arm A). Nothing below
is built.

### What the pages already say

- `lifecycles/states.md` "Resize and roll": "A resize or roll puts the fabric back to pending; meshes
  already serving stay ready-for-traffic. During a bin roll, a member still on its old binary/identity
  does not satisfy its planned replacement. A new binary requires a replacement node with a new NodeId
  and incarnation; node.restart retains the same process and binary. The mesh cannot report
  ready-for-traffic, and the fabric cannot climb, until the roll's last planned birth checks in."
- `node-identity-and-state.md` §7: "Every mesh always has a LISTENING node-admin"; "A node-admin roll
  replaces a non-primary admin first, so the mesh keeps another listening admin before the current
  primary goes; the next primary is whatever the election resolves."
- `build.md` §1 and §4.1: a Build is the complete explicit topology, written once; "Every execution
  attempt re-reads the accepted topology and current realized/observed state and computes remaining
  work. Completed step receipts prevent repeating already-satisfied work; a Build never resumes from a
  mutable instruction pointer." "A primary or executor change during that active Build keeps the same
  `build_id`." `fabric-primary.md`: "A fabric-primary change mid-Build continues the same Build."
- `lifecycles/node-primary-update.md`: "A stop, delete, restart, replace or `node.connections.delete
  { scope: All }` of a primary runs it first"; "The mesh-primary and the fabric-primary move together
  and are never separated"; when both are one node and the successor is in the same mesh, "the
  successor becomes both". `build.md` §12.4: the successor, now the mesh's primary, executes the delete.
- `rdm-core` `RecipeStep::Create { executable }` (node-rpc-envelope.md "Build recipe"; e12.s91): the
  Build fixes the binding a create launches; whichever admin runs or resumes the step launches it,
  re-hashed at the launch, and the node's record keeps it. `lifecycles/node-recovery.md` "Retained": a
  recovered node runs the bin its row records, never the recovering admin's.
- `lifecycles/node-replace.md`: the predecessor half (connections deleted, mesh left, then deleted when
  no rollback is wanted) and "Immutable exe: new bin/budgets require this new node, never a restart of
  the stopped one". `lifecycles/node-delete.md`: delete releases node.id and name, never storage; "a
  later spec can inherit them"; the peer-location book blocks the old `(node.id, incarnation)` "even
  when a successor uses the same endpoint key".

What is missing is the operation: no Build kind binds a target executable set and replaces the members
onto it (the i141 packet, §4), and no admin launches from a set other than the one it started with.

### Ownership

By `build.md`'s ownership test (a mechanism that makes sense for any distributed mesh is RDM's; a
behaviour that depends on the product is Rafka's):

- **RDM owns the roll:** the accepted set and its durable acceptance; every receipt and the resume;
  the order of replacement and every primary's move; a listening node-admin in every mesh throughout;
  launching the exact bound executables and handing the accepted set to every admin; completion only
  after the fabric's rounds. The roll is generic: replace these members with these bindings under the
  lifecycle pages' guarantees.
- **Rafka owns the release:** which set to roll to; whether two releases may serve side by side while
  the roll runs; whether the new executable may open the data dir the old one wrote, and whether the
  old one may open what the new one wrote (its `before_ready` hook reads `BeforeReadyCtx::inherited`,
  `RDM_INHERITED_DATA_DIR`; its `sync_state` hook answers the climb). RDM makes no claim about
  application data: a roll moves executables and identities over kept data dirs, nothing else.

### The operation

1. **Accept.** `build.create { Change(Roll { bindings }) }` at the fabric-primary. `bindings` is a
   `BindingSet`: one path and sha256 per launch id, and for a container fabric the image
   (`rdm-core/src/binding.rs`). The fabric-primary validates it as the bootstrap admin validates its
   own at start (`BindingSet::validate`): every launch id the accepted topology uses declared and
   bound, `node_admin` always; every file present and hashing to its sha; every binding checked
   against the fabric's provider (`check_image`: a process fabric binds no image, a container fabric
   binds its one image). Any defect is refused by name and nothing is accepted. Refused
   `fabric-not-ready` until the fabric first reached ready-for-traffic, as every recipe is; refused
   `build-in-progress` while another Build reconciles, as every topology Build is. Accepted, the set is
   in the Build's acceptance record, written once, and at the acceptance, before the door answers, the
   fabric goes back to pending by the resize hook (`control_plane.rs:503`; states.md "Resize and
   roll"); its meshes stay ready-for-traffic.
2. **The accepted set is the fabric's launch set.** The set is a field of the accepted topology
   (`FabricTopology.bindings`), not of the roll's record alone: every Build compiles from the current
   topology (`accepted.rs:399`, `compile(current, change, observed)`), so a resize, an added node, a
   mesh created or removed after a roll carries the roll's set forward unchanged, and only a `Roll`
   replaces it. Day 0's comes from the bootstrap admin's `RDM_EXECUTABLE_BINDINGS` into B0's topology,
   so the field exists from day 0. What every admin launches from is the current Build's set. A
   launched admin takes the current set from its launcher's entry answer, as it takes the provider
   policy today (`boot.rs:425`), and every admin reads the set from the current Build at each launch,
   never from a value held since its own start: `AdminConfig.bindings` as a lifetime value and the
   forwarding of `bindings_env` to a launched admin (`boot.rs:1275`) go; the bootstrap env is day 0's
   input only. A launch's binding is chosen as today (`runner.rs:273`): the Create step's, else the
   row's, else the current set's for the node's kind. So after a roll an ordinary create (a resize, an
   added node) from a replaced or an unreplaced admin launches the new set, a re-created admin's own
   launches run it, and a node recovery still runs its row's bin (node-recovery.md "Retained"). An
   admin whose executable already hashes to the set's is not replaced and launches from the new set
   all the same: its set is the Build's, not its own. This closes Architect 1's first gap: launching
   an admin's new executable and installing its launch set were two things; the set is now the
   topology's, and its second review's: a Build after the roll inherits the set by the compile rule,
   never by a rule of its own.
3. **The plan is the Build's topology, not a list.** A roll changes no path.name: its compiled
   topology is the current one (every path, mesh id and path config unchanged) with the accepted set
   as every path's target binding, by kind; it is written once in the acceptance record. There is no
   persisted operation list and no instruction pointer (build.md §1). Each attempt computes remaining
   work as the members whose row records a sha256 other than the set's for their kind, or none (a row
   written before the field decodes with none and is replaced), and orders them by a rule read from
   the current primaries at that moment: mesh by mesh; in a mesh its non-primary node-admins, then its
   primary node-admin, then the nodes of each other type, non-primary before primary. A member's
   operations are keyed by its path.name and step, so a receipt `(build_id, attempt, operation, step)`
   names the member and the step, and the exact birth it acted on is the attempt's. Interruptions
   resume from observed state, which is why no plan record is needed: predecessor deleted and the
   successor not yet created (the row at the path.name is Deleted, recording the old sha, the name
   free): the remaining work is the create; successor created and its receipt not written (the row
   names a live birth on the set's sha): nothing remains for it; successor claimed and launched but
   not yet ReadyForTraffic: it is a member of its mesh like any other, and its mesh-primary's node
   investigation reconciles it (reached: it joins; exited: node recovery at the same path.name on the
   row's bin, which is the set's). No member is replaced twice. This closes Architect 1's third gap.
4. **Replace one member** (node-replace.md step 2's deleted arm: "If no rollback is wanted"): the
   predecessor's `node.connections.delete { All }`, `node.mesh.leave`, `node.delete` (which composes
   the whole stop; its identity and name released, its storage kept); then `node.create` at the same
   `path.name`, the Create step carrying the set's binding of its kind, with a new node.id and
   incarnation, over the kept data dir (`inherited`, `RDM_INHERITED_DATA_DIR=1`, the endpoint key
   loaded from it), config and tags from the Deleted record (node-replace.md "Inherited"). The
   peer-location book's block of the old `(node.id, incarnation)` covers a successor on the same
   endpoint key (node-delete.md step 5). The create ends at the member's ReadyForTraffic check-in.
5. **A primary's own replacement.** A primary is replaced last of its type and goes through
   `node.primary.update` first, as every delete or replace of a primary does (node-primary-update.md);
   the successor is a node of the same type already on the set, since the non-primaries went first.
   The two cases the page separates:
   - *The mesh-primary (the `node-admin` type).* The fabric-primary commands the update; the successor
     admin installs itself, writes the row and, now mesh-primary, executes the delete and the create
     (build.md §12.4). When the mesh-primary is also the fabric-primary, both move together to the
     same-mesh successor (node-primary-update.md "Both primaries"); the caller's `build.create` stream
     ends `FabricPrimaryMoved { build_id, fabric_primary }`, the caller resubmits the same build_id
     there, and the new fabric-primary continues the same Build as its next attempt (build.md §4.1),
     then replaces the old fabric-primary's node as an ordinary member.
   - *A primary of any other type (a broker-primary, an rpc primary).* The mesh-primary commands the
     update to another node of that type and stays the executor and the writer; the old primary only
     makes the call and writes nothing. Then it is replaced as an ordinary member. A type with one
     node in the mesh has no successor and no update: its node is replaced as an ordinary member, and
     the election names the new birth when it is ReadyForTraffic ("the next primary is whatever the
     election resolves").
   The fabric's pending from the roll is not `fabric-not-ready`: that refusal is the initial fabric's,
   before its first ready-for-traffic.
6. **A mesh with one node-admin** (R4, Luke 2026-10-24, arm A). Its only admin is its primary and
   has no non-primary to go first, and the page allows no replace that leaves a mesh without a
   listening admin. The roll's compile adds a second node-admin path to such a mesh
   (`<mesh>.admin.<next free>`), created first from the set; the roll waits for it to check in
   ReadyForTraffic, hands the primary to it by `node.primary.update` (both primaries when the original
   is the fabric-primary, step 5), then replaces the original. The added admin stays in the accepted
   topology after the roll: the mesh has the HA shape the page assumes. No temporary member, no
   cleanup. This closes Architect 1's second gap.
7. **Through the mesh-primaries.** The fabric-primary claims each attempt and dispatches
   `build.attempt.run` to the executor `executor_for` names; a run ends `HandedOff { to }` at an
   operation another executor runs, and the fabric-primary's next claim and call carry the run on
   (node-rpc-envelope.md, Build carrier). A member lost during the roll is node recovery: a drift
   attempt of this same Build, run by its mesh-primary on the row's bin; the roll replaces it after,
   like any member (R-BI7: the roll's operation on a mesh and that mesh's drift attempt never both
   open).
8. **Complete.** The roll's Build is `Complete` only after the last planned birth checked in and the
   fabric climbed: state-sync (Rafka's `sync_state` hook, answered at once when no content work is
   needed), state-commit, ready-for-traffic (states.md "Resize and roll"). The fabric leaves pending
   for state-sync when every mesh's planned members, on the set, are ready (build.md §10).

### Decided (Luke 2026-10-24: R1-R3 as written; R4 arm A)

- **R1, the data dir: inherited.** The successor is created over the predecessor's data dir, so a
  restage keeps each node's data. The other arm, a newly allocated dir, makes data transfer Rafka's
  step on every roll.
- **R2, the predecessor: deleted.** A roll goes forward; going back is another roll with the previous
  binding set, over the same kept data dirs. Amended on Architect 1's review, the arm unchanged: that
  is a roll, not a data rollback. Whether the previous executable can open what the new one wrote is
  Rafka's (its release compatibility, read in `before_ready` with `inherited`), and RDM never promises
  it. The other arm, the predecessor kept stopped as `<name>.old` for a rollback window, is not taken.
- **R3, the admins' own binaries.** A replaced node-admin is launched from the set's binding of
  `node_admin` like any member; no running admin swaps its binaries in place. Step 2 is what makes R3
  complete: the admin's launch set follows the Build, so a replaced and an unreplaced admin alike
  launch from the accepted set.

- **R4, the mesh with one node-admin: arm A (Luke 2026-10-24).** Rafka's estates run one
  node-admin per mesh today (`lane_mesh_finish.py`, one `node_admin_api_base` per mesh), so this is
  the common case. The roll creates a second node-admin first, waits for it to become
  ReadyForTraffic, hands over the primary role, then replaces the original; the added admin remains
  in the accepted topology after the roll (step 6). A later `ReconcileMesh` with one admin removes it
  like any resize. Not taken: (b) refusing a one-admin mesh by name and making Rafka resize first;
  (c) a temporary admin deleted after the roll, two more primary moves and a cleanup for the same
  result.

### Wire and storage

`BuildChange::Roll { bindings: BindingSet }`, a new variant of the Build change. The accepted topology
(`FabricTopology`, inside every `BuildAccepted`) carries `bindings`: B0's from the bootstrap admin's
env, carried forward by every compile, replaced by a `Roll` (durable format moved; a record written
before it decodes with none, and an admin whose current Build carries none launches from its start env,
as today, until the fabric's first roll). The entry answer carries the current Build's set beside the
provider policy. No new op, no new storage object, no plan record; the
receipts are the Build's. Removed: `AdminConfig.bindings` as a lifetime value and the forwarding of
`bindings_env` to a launched admin.

### Cells, RED first

- a two-mesh fabric, two admins per mesh, rolled onto a new set: every member at the same `path.name`
  with a new node.id and incarnation, its record naming the set's sha, its data dir inherited and its
  endpoint key kept;
- in each mesh the non-primary admin replaced before the primary, and a listening admin throughout
  (no round misses the mesh's aggregate);
- the fabric-primary's mesh: the caller's stream ends `FabricPrimaryMoved`, the resubmitted build_id
  completes at the new fabric-primary, the old fabric-primary's node re-created on the set;
- a mesh with one admin: a second admin created first, on the set, ReadyForTraffic and primary before
  the original is replaced; it is in the accepted topology after the roll;
- a non-admin primary (a broker-primary with another broker in its mesh): the primary moves to the
  other broker by `node.primary.update` with the mesh-primary as writer, then the old primary is
  replaced; a type with one node is replaced with no update and the new birth is its primary once
  ReadyForTraffic;
- roll, then a resize, then an admin restart, then an ordinary `node.create`: every launch runs the
  roll's set, which the resize's Build carried forward;
- the executor killed after a predecessor's `node.deleted` and before its create: the next attempt
  creates it once; killed after the successor's check-in and before its receipt: the next attempt runs
  nothing for it; no member replaced twice;
- after the roll, an ordinary `node.create` (a resize) from a replaced admin and from an unreplaced
  admin launches the set's binary, and a node recovery runs its row's;
- the Build `Complete` only after the fabric's ready-for-traffic round;
- refused `fabric-not-ready` before first ready-for-traffic; a sha that does not match, a launch id the
  topology uses with no binding, and a container fabric's set without the image, each refused by name
  with nothing accepted.

Performance acceptance stays separate (the ruling): equivalent restages before and after, same rebuild
scope, deploy plus tests measured.
