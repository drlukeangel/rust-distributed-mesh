# Proposal: a node's executable replacement

Status: approved by Luke 2026-11-04 (Q377, Q378, Q379, Q380, Q381); for the rafka session's
approval. The lead. The minimum that lets a developer redeploy a node's binary (Q380: "do the
minimum possible to achieve the goal"). Statements about today's code are read from main
`13467ebac`; the build branches from Rafka's pin `8277a5633`.

## 1. The use case

An existing mesh; a developer changed the gateway and redeploys the gateway binary. Today there is
no one operation for it.

## 2. Today

- build-deployment.md §3 and build.md §12.5: "A new executable is a new node: a Build that adds it,
  `Create { nodes }` naming the executable, at a path and name no live node holds." The caller
  composes the primitives itself, each its own call (Q345).
- rdm-admin `accepted.rs` `created()`: a Create naming a path and name the records hold is
  `BuildReject::NameHeld`; node-create.md step 1 says the same.
- Nobody owns the order of the caller's calls: Rafka's restage of a broker binary answered
  `422 no-secondary: cannot transfer: no secondary of type broker in mesh mesh1` (`control_plane.rs`
  `last_admin`, `BuildReject::NoSecondary`): the door had deleted every broker's connections first.
- A Build's Delete keeps the old data dir on disk (node-delete.md "Retain the data dir; a later node
  at the path can inherit it"); only node recovery's create names it for the new node
  (`deployment/pipeline.rs`, `LaunchOf::Recovery`, the create request's `data_dir` and
  `predecessor`). A redeployed broker loses its storage today.

## 3. The change

`build.create` stays explicit (Q380): `Create { nodes }` adds, `Delete { node_ids }` deletes, and a
node the request does not name is never touched. One rule is added:

**A `Create` naming a held path and name with another executable is a node's executable
replacement** (Q377, Q378). With the same executable it stays `NameHeld`. "Another executable" is a
different executable binding: its path, sha256 or image (build-deployment.md §2), so a rebuilt
binary at the same path, with its new sha256, is another executable.

`node.create` follows the same rule for one node (Q378). A node whose executable is the same and
whose config or tags differ is not a replacement: the caller sends `node.config.update` (and the tags
update), as today (Q379).

### 3.1 The replacement, at the node's mesh-primary

1. **The primary role first.** If the node holds its node-type primary and another node of its type
   is in its mesh, `node.primary.update` moves the role to that node (node-primary-update.md). For a
   node-admin that holds the mesh-primary or the fabric-primary role, the role moves as
   node-primary-update.md's C1 and C2 move it, and the new mesh-primary runs steps 2 and 3 (Q373).
   A type's only node skips this step (`last_admin`, R5): its type has no primary until the new node
   is Ready.
2. **`node.delete`** of the old birth, the ordinary commanded delete; its runtime ends before
   step 3 launches.
3. **`node.create`** at the same path and name: a create in every respect.

| | the new node |
|---|---|
| path, name | the same |
| node.id, incarnation | new |
| endpoint_id | new, written into its data dir, never loaded from it (Q358) |
| transport_addr and every listener port | new |
| executable | the submitted one |
| config, tags | the submitted entry's (Q377) |
| data dir | the old node's when the submitted entry's `storage_persistence` is `Persistent` and the directory exists (Q378, Q381); otherwise new |

The create request names the predecessor and its data dir as node recovery's does; its span names
which data dir it took and why.

### 3.2 Order inside one Build

In each mesh's section: the replacements of a type's other nodes first, its node-type primary last,
so the role moves to a node already on the new executable; node-admins after every other type, the
mesh-primary last. A replacement whose create fails after its delete ends the Build `incomplete` at
that create, and a resubmit runs what is not complete (Q372).

## 4. The RDM side

| change | where |
|---|---|
| a held path and name with another executable is accepted as a replacement | rdm-admin `accepted.rs` `created()` |
| the replacement's three steps, one workflow for both ways in | rdm-admin: the Build's section plan and the node-op executor |
| the create names the old node's data dir outside recovery | rdm-admin `deployment/pipeline.rs`: `data_dir` and `predecessor` for a replacement |
| `node.create`'s held-name rule | rdm-admin node-create admission |

No wire change: no new `BuildChange` variant and no new field.

## 5. The Rafka side

- To redeploy a broker's binary, Rafka's door sends one `build.create` with `Create { nodes }`
  naming the broker's path and name with the new executable, each entry whole as it read it. It
  composes no node commands: no connection delete, stop, rename or delete of its own.
- The replaced broker is a new node at the same path and name: a new node.id, incarnation, endpoint,
  address and listener ports. Rafka reads them from the new node's record after its Ready, and
  anything Rafka keys by node.id sees a new node.
- A broker declared `Persistent` gets its data dir back; one declared `Ephemeral` starts empty.

## 6. Page text (approved with the change)

- build-deployment.md §3: "A new executable is a new node: a Build that adds it, `Create { nodes }`
  naming the executable, at a path and name no live node holds (build.md §12.5)." → "A new executable
  is a new node at the same path and name: a `Create` naming a held path and name with another
  executable is a node's executable replacement, which RDM runs: the primary role first, `node.delete`
  of the old birth, `node.create` at the same path and name."
- build-deployment.md §3: "To keep the old node's data, the old node is stopped ... no page documents
  them together (Q345)." → "The new node takes the old node's data dir when its own
  `storage_persistence` is `persistent` and the directory exists; otherwise it gets a new one."
- build.md §12.5: "at a path and name no live node holds ... These are primitives, each its own call."
  → "at the same path and name: RDM replaces the node, the primary role first, then delete and
  create (Q377, Q378)."
- node-create.md step 1: "`NameHeld { by: node.id }` refuses a held name and nothing is launched." →
  "`NameHeld { by: node.id }` refuses a held name with the same executable and nothing is launched; a
  held name with another executable is a node's executable replacement (build-deployment.md §3)."
