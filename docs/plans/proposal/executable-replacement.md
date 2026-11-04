# Proposal: a node's executable replacement

Status: **for Luke's review; nothing is built until he says go.** The lead, 2026-11-04, from Q377
and Q378. Statements about today's code are read from main `08c3211e4`.

## 1. The use case

An existing mesh; a developer changed the gateway and wants to redeploy the gateway binary. Today
there is no one operation for it.

## 2. What the pages and the code do today

- build-deployment.md §3 and build.md §12.5: "A new executable is a new node: a Build that adds it,
  `Create { nodes }` naming the executable, at a path and name no live node holds." The caller
  composes the primitives itself, each its own call (Q345): a Delete of the old node (or a stop and
  `node.name.update`), then the Create.
- node-create.md step 1: "`NameHeld { by: node.id }` refuses a held name and nothing is launched";
  rdm-admin `accepted.rs` `created()` returns `BuildReject::NameHeld`.
- Nobody owns the order of the caller's calls. Rafka's restage of a moved broker binary answered
  `422 no-secondary: cannot transfer: no secondary of type broker in mesh mesh1`, 3 of 3: the door
  deleted every broker's connections first, so the primary's delete found no ready broker
  (`control_plane.rs` `last_admin`, `BuildReject::NoSecondary`).
- Storage: a Build's Delete keeps the old data dir on disk (node-delete.md "Retain the data dir; a
  later node at the path can inherit it"), but a Build's Create never names it, so the new node gets
  a new one. Only node recovery's create names the predecessor's data dir
  (`deployment/pipeline.rs`, `LaunchOf::Recovery`, the create request's `data_dir` and
  `predecessor`; node-recovery.md step 3). A redeployed broker loses its storage today.

## 3. The rulings

- **Q377.** `build.create` takes the complete topology and RDM diffs it against what it holds: a
  node present with the same executable is untouched; present at the same path and name with another
  executable is replaced, RDM's own delete then create at that name, in RDM's order; absent is
  deleted; new is created. Everything in the submitted entry replaces the held row. The caller
  composes no node commands.
- **Q378.** One workflow for `build.create` and `node.create`; a Build is a collection of commands.
  The new node takes its predecessor's data dir when its `storage_persistence` is `persistent`.

## 4. The workflow: a node's executable replacement

Run at the node's mesh-primary, for one node (commands go down; Q373: every command for a mesh goes
to its primary of the moment):

1. **The primary role first.** If the node holds its node-type primary and another node of its type
   exists in its mesh, `node.primary.update` moves the role to that node. For a node-admin that
   holds the mesh-primary or the fabric-primary role, the role moves as node-primary-update.md's
   C1 and C2 move it, and the new mesh-primary runs steps 2 and 3. A type's only node skips this
   step: its delete needs no successor (`last_admin`, R5), and its type has no primary until the new
   node is Ready.
2. **`node.delete`** of the old birth: the ordinary commanded delete. Its runtime is terminated
   before step 3 launches anything, so no two processes use one data dir.
3. **`node.create`** at the same path and name, a create in every respect:

| | the new node |
|---|---|
| path, name | the same |
| node.id, incarnation | new |
| endpoint_id (key) | new, written into its data dir, never loaded from it (Q358) |
| transport_addr | new (port 0) |
| listeners (ports) | new: bound again by the new node, never the old node's ports (Luke, 2026-11-04) |
| executable | the submitted one |
| config, tags | the submitted entry's (Q377) |
| data dir | the predecessor's when `storage_persistence` is `Persistent` and the directory exists (Q378); otherwise new |

The create request names the predecessor and its data dir as node recovery's does (`data_dir`,
`predecessor`); the create's span names which data dir it took and why.

## 5. The two ways in

### 5.1 `build.create` with the complete topology (Q377)

The Build's change is the complete topology: every mesh and every node, each node whole. RDM
compares each submitted node with the node it holds at that path and name:

| submitted | held | the Build runs |
|---|---|---|
| a node at a path and name | none | `node.create` |
| none | a node | `node.delete` |
| the same executable, the same entry | the node | nothing |
| the same executable, another config or tags | the node | `node.config.update` (and the tags update) on the running node (Q379) |
| another executable | the node | the replacement (§4) |

Order in each mesh: replacements of the other nodes of a type first, its node-type primary last, so
the primary moves to a node already on the new executable; node-admins after the other types, the
mesh-primary last. Each mesh's section runs at its mesh-primary of the moment (Q373). The Build's
states and its records are the ones it has today (pending, running, complete, incomplete, failed);
a replacement whose create fails after its delete ends the Build incomplete at that create, and a
resubmit runs what is not complete (Q372).

### 5.2 `node.create` at a held path and name

A `node.create` naming a held path and name with another executable runs §4 for that one node. With
the same executable it is refused `NameHeld` as today: there is nothing to replace.

## 6. The RDM side

| change | where | wire |
|---|---|---|
| the complete-topology change on `build.create` | rdm-core `BuildChange`: a new variant appended at position 3 (name O1) | **yes**: appended; an old build refuses it by name |
| the diff of §5.1 | rdm-admin `accepted.rs` (`created()` stops refusing a held name with another executable) and the Build's section plan | no |
| the replacement's three steps | rdm-admin's node-op executor, one workflow both §5.1 and §5.2 call | no |
| the create names the predecessor's data dir outside recovery | `deployment/pipeline.rs`: `data_dir` and `predecessor` set for a replacement as for `LaunchOf::Recovery` | no |
| `node.create`'s held-name rule | rdm-admin node-create admission | no |

## 7. The Rafka side

- Rafka's door reads the topology (`fabric.topology.get`), changes what it wants (a broker's
  `executable`), and sends it whole with `build.create`. It sends each entry exactly as it read it.
- A replaced node's listeners are new: a gateway's Kafka and API listener ports are the new node's,
  never the old node's. Rafka reads them from the new node's record after its Ready.
- It composes no node commands: no connection delete, stop, rename, delete or create of its own.
- A replaced node is a new node at the same path and name: a new node.id, incarnation, endpoint and
  address. Anything Rafka keys by node.id sees a new node.
- Rafka changes nothing until the RDM pin that carries this lands; the request shape goes to the
  rafka session with that pin.

## 8. Page text for Luke's yes (exact old → new)

- build-deployment.md §3: "A new executable is a new node: a Build that adds it, `Create { nodes }`
  naming the executable, at a path and name no live node holds (build.md §12.5)." → "A new executable
  is a new node at the same path and name: a node's executable replacement, which RDM runs when a
  Build's topology names another executable for a held path and name, or a node.create does (§4 of
  the replacement page)."
- build-deployment.md §3: "To keep the old node's data, the old node is stopped ... no page documents
  them together (Q345)." → "The new node takes the old node's data dir when its
  `storage_persistence` is `persistent` and the directory exists; otherwise it gets a new one."
- build.md §12.5: "at a path and name no live node holds ... These are primitives, each its own call."
  → "at the same path and name: RDM replaces the node, delete then create, in its own order (Q377,
  Q378)."
- node-create.md step 1: "`NameHeld { by: node.id }` refuses a held name and nothing is launched." →
  "`NameHeld { by: node.id }` refuses a held name with the same executable and nothing is launched;
  another executable is the replacement."
- A new lifecycle page `lifecycles/node-executable-replacement.md` with §4's steps, and its row in
  the lifecycle index.

## 9. Open for Luke

- **O1.** The name of the complete-topology change on `build.create`.
- **O2, ruled Q379 (A):** a node with the same executable and another config or tags gets
  `node.config.update` (and the tags update) on the running node, never the replacement.
