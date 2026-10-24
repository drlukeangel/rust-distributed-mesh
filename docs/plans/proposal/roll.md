## Roll: a running fabric's members onto a new executable set, as one Build

Proposal for i143, for Luke's yes before anything is built (Luke 2026-10-22, arm (a) of the i141 B3
packet, rafka-v2 `36781090eb`: "RDM owns the roll"). Nothing below is built.

### What the pages already say

- `lifecycles/states.md` "Resize and roll": "A resize or roll puts the fabric back to pending; meshes
  already serving stay ready-for-traffic. During a bin roll, a member still on its old binary/identity
  does not satisfy its planned replacement. A new binary requires a replacement node with a new NodeId
  and incarnation; node.restart retains the same process and binary. The mesh cannot report
  ready-for-traffic, and the fabric cannot climb, until the roll's last planned birth checks in."
- `node-identity-and-state.md` §7: "A node-admin roll replaces a non-primary admin first, so the mesh
  keeps another listening admin before the current primary goes; the next primary is whatever the
  election resolves." Every mesh always has a listening node-admin.
- `rdm-core` `RecipeStep::Create { executable }` (node-rpc-envelope.md "Build recipe"; e12.s91): the
  Build fixes the binding a create launches; whichever admin runs or resumes the step launches it,
  re-hashed at the launch, and the node's record keeps it.
- `lifecycles/node-replace.md`: the predecessor half (connections deleted, mesh left, stopped) and
  "Immutable exe: new bin/budgets require this new node, never a restart of the stopped one".

What is missing is the operation: no Build kind binds a target executable set and replaces the members
onto it (the i141 packet, §4).

### The operation

1. **Accept.** `build.create { Change(Roll { bindings }) }` at the fabric-primary. `bindings` is a
   `BindingSet`: one path and sha256 per launch id (`rdm-core/src/binding.rs`). The fabric-primary
   validates it as a launch does (`BindingSet::validate`: every path present, every sha matching); a
   set that does not validate is refused by name and nothing is accepted. Refused `fabric-not-ready`
   until the fabric first reached ready-for-traffic, as every recipe is. Accepted, the set is in the
   Build's acceptance record, so every admin that runs or resumes the Build launches from it.
2. **Plan.** The fabric-primary compiles the roll once against the accepted topology into the members
   to replace, mesh by mesh. In a mesh: its non-primary node-admins first, then its primary node-admin
   (its seats move first by the existing `node.primary.update`, Part C1), then the other kinds, each
   cohort's non-primary members before its primary. A member whose record already names the target
   binding of its kind is not in the plan.
3. **Replace one member** (operations of the Build, each idempotent by its natural key): the
   predecessor's `node.connections.delete { All }`, `node.mesh.leave`, `node.stop`, then `node.delete`
   (its identity and name released, its storage kept); then `node.create` at the same `path.name`
   with the Build's binding of its kind, a new node.id and incarnation, over the predecessor's data dir
   (inherited, `RDM_INHERITED_DATA_DIR=1`), config and tags from the Deleted record (node-replace.md
   "Inherited"). The create ends at the member's ReadyForTraffic check-in.
4. **Through the mesh-primaries.** The fabric-primary opens the attempts; each mesh's operations run
   at its mesh-primary, the existing hand-off between executors carrying the run from mesh to mesh
   (`build.attempt.run`'s `hand_off_to`). A mesh-primary's own replacement runs after its seats moved,
   at the next primary.
5. **Resume.** An interrupted roll is the next attempt of the same Build: completed operations are in
   the Build log by their receipts and are not run again; a member already re-created on the target
   binding is skipped; a half-done member resumes at its first missing receipt.
6. **Complete.** The roll's Build is `Complete` only after the last planned birth checked in and the
   fabric climbed again: state-sync (Rafka's `sync_state` hook), state-commit, ready-for-traffic
   (states.md "Resize and roll").

### Choices, for Luke's yes (recommendation first)

- **R1, the data dir. Recommendation: inherited.** The successor is created over the predecessor's
  data dir, so a restage keeps each node's data. The other arm, a newly allocated dir, makes data
  transfer Rafka's step on every roll.
- **R2, the predecessor. Recommendation: deleted.** A roll goes forward; going back is another roll
  with the previous binding set. The other arm keeps it stopped as `<name>.old`, as replace allows,
  for a rollback window the restage does not use.
- **R3, the admins' own binaries.** A replaced node-admin is launched from the Build's binding of the
  admin's launch id like any member, so after a roll every admin runs the new binary and launches from
  the Build's set. No running admin swaps its binaries in place. Recommendation: this, with no second
  mechanism.

### Wire and storage

`BuildChange::Roll { bindings: BindingSet }`, a new variant of the Build change (durable format moved:
the Build log's acceptance record carries it; a log written before it decodes as before). No new op,
no new storage object; the receipts are the Build's.

### Cells, RED first

- a two-mesh fabric rolled onto a new binding set: every member at the same `path.name` with a new
  node.id and incarnation, its record naming the new binding, its data dir inherited;
- in each mesh the non-primary admin replaced before the primary, and a listening admin throughout;
- the executor killed mid-roll: the next attempt completes it, no member replaced twice;
- the Build `Complete` only after the fabric's ready-for-traffic round;
- refused `fabric-not-ready` before first ready-for-traffic, and a set whose sha does not match refused
  by name with nothing accepted.

Performance acceptance stays separate (the ruling): equivalent restages before and after, same rebuild
scope, deploy plus tests measured.
