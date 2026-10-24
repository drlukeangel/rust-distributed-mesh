## Executable bindings: set at node.create only

Proposal for i143 (the lead, 2026-10-24), for Luke's yes before any build. It replaces the roll
([roll.md](roll.md), landed `614d01653..5e070d089`, `9fbf7efba`), which added a Build change
(`BuildChange::Roll`) and a fabric-level launch set (`FabricTopology.bindings`). Luke, 2026-10-24:
"bindings aren't attached to the fabric they're attached to the node"; "it's rebirth only"; "create only".

An **executable binding** is a launch id mapped to an executable path and its sha256, and under the
container provider the image (`node-rpc-envelope.md:1409`, `deployment-provider.md:264`).

### 1. The rule

A node's binding is an input of `node.create` and of nothing else. The birth runs it for its whole life:
`node.restart` keeps the bin (node-restart.md: "Kept: … bin and data dir"; "New executable: Requires a
new node through Rafka's replace recipe"). No operation changes a running node's binding.

- **node.create** names the binding the birth launches (`RecipeStep::Create { executable }`,
  `node-rpc-envelope.md:1409`). The launch re-hashes the file and launches it only on a match
  (`deployment-provider.md:264`). The create's claim records it on the node's row (R6, `11faeb419`).
- **A new binary is a re-birth:** Rafka's replace recipe (node-replace.md), its `Create` step naming the
  new binding. A new node.id at the same `path.name`.
- **Every node to a new binary** is one recipe Build of replace recipes, composed by Rafka in the order
  it chooses; RDM executes each step as its op's own workflow and never runs a completed step again
  (build.md §1).
- **Node recovery** re-creates at the same `path.name` running the binding the lost birth's row records
  (node-recovery.md "Retained"). A recovery is a create; the binding it names is the row's.
- **Day 0:** the bootstrap admin records its own running binding on its row (R6); `RDM_EXECUTABLE_BINDINGS`
  is day 0's input for the binding of each node the day-0 Build creates.

### 2. Removed

- `BuildChange::Roll` and `TopologyChange::Roll` (the Build wire is positional: `durable format moved`).
- `FabricTopology.bindings` (the fabric-level launch set), its carry-forward in every compile, and every
  read of it at launch (`runner.rs` `launch_set`).
- The roll's completion hold (`fabric-climb` for a roll) and its plan (`roll_progress`,
  `roll_operations`, the interrupted-delete arm of `9fbf7efba`).
- The roll's cells (`roll::`). Kept from the roll's landing, as they are node rules: R6 (every row
  records what its birth runs; the bootstrap's running executable held to its configured `node_admin`
  sha), R5's page sentence (a type's only node needs no primary update), the container provider
  mounting a launched admin's bound executables read-only at their host paths (`LaunchTemplate::reads`),
  `JoinControl`'s launch set if a launched admin still needs day 0's set for its own creates (§3).

### 3. The question this leaves

**Q157: a create that names no binding.** A topology Build (a resize, an added node, a mesh created)
creates nodes from the accepted topology, which today names no binding per node.

- **A (recommended): the topology carries each node's binding.** The accepted topology's node entry
  names the binding the create launches; a topology change that adds nodes names theirs (Rafka knows the
  binaries it deploys); a node already created keeps its row's. Bindings stay per node, set only at
  create. Durable: the topology's node entry gains the field (`durable format moved`).
- **B: the create inherits its kind's binding from a sibling's row** in the mesh, else day 0's input.
  No new field; the create infers.
- **X: something else.**

### 4. Cells, RED first

- a node created with a named binding runs it and its row records it; a restart keeps it;
- a replace recipe whose `Create` names a new binding: a new node.id at the path running the new binary;
- a recipe replacing every node of a two-mesh fabric, the fabric-primary moving once;
- a recovery runs the binding the lost birth's row records;
- under Q157 A: a resize whose new node names its binding runs it; a create naming no binding refused by name;
- a `Roll` change refused as unknown (`Change::Roll` gone).

### 5. Pages, each with exact old and new text for Luke's yes before it lands

- deployment-provider.md: a binding section stating §1.
- node-create.md: the binding is the create's input, recorded on the row, never changed for the birth.
- node-replace.md: the successor's `Create` names its binding (a new binary is a re-birth).
- CLAUDE.md's env table: `RDM_EXECUTABLE_BINDINGS` and `RDM_EXECUTABLE_CANDIDATE` as day 0's input.
- [roll.md](roll.md) marked superseded by this page.
