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
   `PathConfig { storage, placement }`; it gains `executable`. A Build request that creates nodes names
   each path with its config, executable included. There is no per-type section in RDM and no stored
   default anywhere: a created path whose config names no executable is refused by name at acceptance,
   nothing runs. Per-type convenience is Rafka's door's: its `shape_bins` is per type and the door
   expands it into per-path entries before the typed call.
2. **Compiled at acceptance, explicit after it.** The fabric-primary compiles the request against the
   current topology; every created path's entry holds its executable and is never re-read from the
   request, as counts are never re-read (build.md §1). The accepted topology is the one durable answer
   to "what should this node run", beside the shape.
3. **Recorded on the row at the claim.** The create's name claim records the executable on the node's
   row (R6, landed `11faeb419`), the bootstrap admin's own included (its running executable, hashed,
   held to its configured `node_admin` sha). The row is the record of what this birth runs.
4. **Re-hashed at the launch.** The launching admin hashes the file and launches only on a match; a
   missing file or a differing hash is refused by name and nothing else is launched in its place.
5. **Replaced only by re-birth.** A request that names a different executable on an existing path is a
   planned replacement, not a refusal: RDM realizes it as node.delete then node.create at the path, a
   new node.id and incarnation, in RDM's order with RDM's guards (admins before the primary, the
   fabric-primary's move and the caller's resubmit on `FabricPrimaryMoved`, R5's one-node type, the
   no-successor refusal). `node.restart` keeps the executable (node-restart.md "Kept"). Rafka submits
   the shape; it composes no steps.
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
  at launch (`runner.rs` `launch_set`); the plan and progress of the removed operation (`roll_progress`,
  `roll_operations`, the interrupted-delete arm of `9fbf7efba`) and its cells; `docs/plans/proposal/roll.md`.
- **Kept:** R6 (every row records what its birth runs); R5's page sentence (a type's only node needs no
  primary update; its predecessor half is one `node.delete`, never a standalone connections delete,
  which the executor refuses on a type's only node); Q154's generic `fabric-climb` hold for every Build
  that puts the fabric to pending, only the removed operation's branch of it goes.
- **Corrected on Architect 1's review:** a containerized admin loses the launch set that told it which
  directories to mount, so executable directories become the admin's launch configuration: read-only
  mounts at stable host paths, given at the admin's launch, and an executable outside them is refused
  by name at acceptance, before anything destructive (A1 gap 1). Rows written before R6 decode with no
  executable: their nodes recover on the executable the current entry names for the path, once, and the
  recovered row records it; a replacement request for such a node is refused by name until it has
  recovered or restarted onto a recorded executable (A1's pre-R6 gap). A topology edit that changes an
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
paths, a row in CLAUDE.md's env table; an executable outside them is refused by name at acceptance.
`RDM_BIN_DIR` stays only as where the harness and the bootstrap find `rdm-node-admin` itself, never a
launch source: the built-in mode that launched a kind's binary from `bin_dir` with no sha goes with the
file, since every create now names its executable.

### 3. Pages that change, each with exact old and new text for Luke's yes before it lands

- **New: `docs/architecture/build-deployment.md`**, the owning page for what a Build deploys: the
  executable's path from the request's per-path entry through acceptance, the row, the launch's hash,
  replacement by re-birth, recovery from the row, and the reads; its relation to build.md (the engine)
  and deployment-provider.md (the provider); the container's executable directories; R6; the refusals by
  name. Linked from fabric-mesh-node-lifecycles.md.
- **build.md** §1: a path's config carries its executable; executables compile per path at acceptance
  and are never re-read; a changed executable on an existing path is a planned replacement; a created
  path without one is refused by name. §4.1: the topology map's per-path entry names the executable.
  §12: a case "replace a node's executable". A link to build-deployment.md.
- **deployment-provider.md**: the binding paragraph (line 264) points at build-deployment.md; §2.2 the
  container provider's executable directories as launch configuration; Q150's §2.3 sentence as ruled.
- **node-create.md**: the Spec row names the executable as the create's input from the path's config,
  recorded on the row at the claim, re-hashed at the launch, never changed for the birth.
- **node-replace.md**: the successor's `Create` names its executable; "Immutable exe" stands.
- **node-recovery.md** "Retained": unchanged in rule; gains the pre-R6 sentence.
- **states.md** "Resize and roll": the heading and the paragraph rewritten without the banned word, as
  "Resize and executable replacement", with Q154's sentence; the rule that a member still on its old
  executable does not satisfy its planned replacement stays.
- **node-rpc-envelope.md**: the Build change wire (path config gains `executable`; the two reserved
  slots); `fabric.nodes.get`'s mapping; `NodeView.executable`; the "Build recipe" text.
- **CLAUDE.md**: the env table rows for `RDM_EXECUTABLE_BINDINGS` and `RDM_EXECUTABLE_CANDIDATE`
  removed; a row for `RDM_EXECUTABLE_DIRS`; `RDM_BIN_DIR`'s row narrowed to where `rdm-node-admin` is
  found; the banned word added to the vocabulary (done, `e76fb7374`).
- **Records:** the PRD's R-D1 row; the rulings index (Q157); rafka-v2's i141 B3 packet and the manifest
  rollout (`needs-ownership-under-rdm.md` §2.4), which rode the removed operation, for the lead to update.

### 4. Cells, RED first

- a node created with a named executable runs it and its row records it; a restart keeps it;
- a request naming a different executable on an existing path: a new node.id at the path running the new
  one, the old birth deleted first, in RDM's order; the fabric-primary's own replacement moving it once;
- a request replacing every node of a two-mesh fabric: Complete only after the ready-for-traffic round
  (Q154), Blocked naming the climb meanwhile;
- a one-broker mesh's broker replaced: one `node.delete` then the create, no refusal at any step;
- a recovery runs the executable the lost birth's row records, with a newer entry pending on the path;
- a created path naming no executable refused by name at acceptance, nothing run; a missing file or a
  differing hash refused by name at the launch;
- a container fabric: an executable outside the admin's configured directories refused by name at
  acceptance; one inside, from a directory absent from day 0's input, launched;
- a row written before R6: recovered once on the current entry, the row then recording it; a replacement
  request for it refused by name until then;
- `node.get` and `fabric.nodes.get` return each node's executable beside its status; `node.topology.get`
  and the digest carry none;
- a `Roll` change refused as unknown; the Build wire's reserved slots decode as before.
