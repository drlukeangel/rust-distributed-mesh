# Proposal: a node's executable replacement

Status: approved by Luke 2026-11-04 (Q377, Q378, Q379, Q380, Q381); red-teamed; for the rafka
session's approval. The lead. The minimum that lets a developer redeploy a gateway's or a broker's
binary (Q380: "do the minimum possible to achieve the goal"). Code is read from main `ea9168188`; the
build branches from Rafka's pin `8277a5633` and lands on main as well. Lead decisions D10–D15 are in
`docs/plans/lead-decisions-2026-11-04.md`.

## 1. The use case

An existing mesh; a developer changed the gateway (or a broker) and redeploys its binary. Today there
is no one operation for it.

## 2. Today

- **The page.** build-deployment.md §3 and build.md §12.5: "A new executable is a new node: a Build
  that adds it, `Create { nodes }` naming the executable, at a path and name no live node holds." The
  caller composes the primitives itself, each its own call (Q345).
- **Acceptance.** build.md §2's gate "A created node's path and name are free" answers `name-held`
  (rdm-admin `accepted.rs` `created()`, `BuildReject::NameHeld`).
- **The plan.** A Create's accepted topology names the path. `plan_with` counts a live birth at a
  named path as satisfying it, and `surplus_births` plans a delete only for a path the topology no
  longer names. So relaxing `created()` alone would accept the Build and run nothing.
- **Nobody owns the order.** Rafka's restage of a broker binary answered `422 no-secondary`
  (`control_plane.rs` `last_admin`, `BuildReject::NoSecondary`): it composed the calls itself.
- **The data dir.** A Build's create over an unreached birth already names `data_dir` and
  `predecessor` (`runner.rs` `create_over`, `LaunchOf::Create`). It does so only when the
  predecessor's runtime reads `exited` and the predecessor's own `storage_persistence` is
  persistent, and a resubmit after the delete has completed passes no data dir.

## 3. The change

`build.create` stays explicit (Q380): `Create { nodes }` adds, `Delete { node_ids }` deletes, and a
node the request does not name is never touched. `node.create` is `build.create`'s `Create`
(node-rpc-envelope.md), so the one rule below covers both (Q378). One rule is added:

**A `Create` naming a held path and name whose node is not of the node-admin type, with an
executable other than the one its row records, is a node's executable replacement.**

- **"Another executable":** a different binding (path, sha256 or image), compared with the
  executable the node's own row records (build-deployment.md §2: "the row is the record of what this
  birth runs"), never with the view or the accepted topology. `launch_id` is not compared.
- **The same executable** stays `name-held`, as today.
- **A node-admin at a held path** stays `name-held` in this change (D10). Its replacement involves
  the fabric-primary (Q370) and a mesh's only node-admin (node-primary-update.md "No-successor
  cases"), and a later change takes it up.
- **Config or tags only:** the caller sends `node.config.update` (and the tags commands), as today
  (Q379).

### 3.1 At acceptance (the fabric-primary)

- The gate "A created node's path and name are free" lets a held path through when the rule above
  holds.
- The held birth `(node.id, incarnation)` is **pinned** as a `Delete`'s are, so every attempt plans
  that birth's delete and tells it apart from a later birth at the path. That later birth may be node
  recovery's successor of this Build's own create (Q372).
- The pinned birth's deletion goes through the same gates a `Delete` does, `last_admin` among them.

### 3.2 The plan (the mesh's section, at its mesh-primary of the moment, Q373)

For each replaced path:

1. **The primary role first.** If the pinned birth holds its node-type primary and another node of
   its type is in its mesh, `node.primary.update` moves the role to that node. A type's only node
   skips this step ("No-successor cases"), and its type has no primary until the new node is Ready.
2. **`node.delete`** of the pinned birth, the ordinary commanded delete. Its runtime ends before
   step 3.
3. **`node.create`** at the same path and name, the pinned birth as its predecessor.

Order:
- A section runs its replacements **one at a time**: each path's three steps finish, its new node
  Ready, before the next path starts.
- The paths of one type run with the node-type primary's last, so the role moves to a node already
  replaced whenever the type has another node in the Create.
- Replacements never join the batch of up to 8 concurrent creates (Q245).

### 3.3 The new node

| | the new node |
|---|---|
| path, name | the same |
| node.id, incarnation | new |
| endpoint_id | new, written into its data dir, never loaded from it (Q358) |
| transport_addr and every listener port | new |
| executable, config, tags | the submitted entry's (Q377) |
| data dir | the pinned birth's when the **submitted entry's** `storage_persistence` is `Persistent` and the directory exists (Q378, Q381); otherwise new |

- The create launches as a `LaunchOf::Create` with the entry's own snapshot (executable, config and
  tags), never recovery's predecessor snapshot. The data dir is the only thing it takes from the
  pinned birth.
- Step 2 has ended the pinned birth's runtime, so the dir is free. The create does not wait for an
  `exited` reading.
- A resubmit after step 2 completed still names the pinned birth's dir: the pin is on the accepted
  topology, so every attempt holds it.
- The create's span names the dir it took and why.

### 3.4 The fabric-primary's check

At a section's end, the fabric-primary checks that every pinned birth of that section is deleted. A
section reported complete with a pinned birth still live ends `incomplete` with a named reason: the
replacement was not run. That is a mesh-primary on an older RDM, which plans "live means
satisfied".

### 3.5 A failure, and a resubmit

A replacement whose create fails after its delete ends the Build `incomplete` at that create. A
resubmit of the same Build runs what is not complete (build.md §2, Q346), with the pinned dir.

## 4. The RDM side

| change | where |
|---|---|
| the held-path gate lets a non-admin path through when its row's executable differs; the held birth pinned | `accepted.rs` `created()`; `control_plane.rs` (pins and gates as for `Delete`) |
| the plan: primary role, delete, create, per replaced path, one at a time | `accepted.rs` `plan_with` / `surplus_births`; the section's executor order (`executor.rs`) |
| the create over the pinned birth's dir | `runner.rs` `create_over` and its `Clear { gone }` arm: the submitted entry's persistence, no `exited` gate after the step-2 delete, the pinned dir on a resubmit |
| the section-end check | the Build drive at the fabric-primary |

- **No wire change:** no new `BuildChange` variant, no new field, no new refusal.
- **The branch from `8277a5633`:** it carries only this change. The landing on main rebases it over
  main, which already carries Q372 and Q373.

## 5. The Rafka side

- **To redeploy a gateway's or a broker's binary,** Rafka sends one `build.create` with
  `Create { nodes }`. It names the node's path and name with the new executable, each entry whole,
  as Rafka read it. It composes no node commands: no connection delete, stop, rename or delete of its
  own.
- **The replaced node is a new node at the same path and name.** It has a new node.id, incarnation,
  endpoint, address and listener ports. Rafka reads them from the new node's record after its Ready,
  and anything Rafka keys by node.id sees a new node.
- **Storage:** a node whose entry says `Persistent` gets its data dir back; one that says `Ephemeral`
  starts empty.
- **A node-admin's binary** is not replaced by this change.

## 6. Page text (approved with the change)

- **build-deployment.md "1. The rule":**
  - Old: "A new executable is a new node: a Build that adds it (Q345)."
  - New: "A new executable is a new node: a Build that adds it, or, at a held path and name of a
    node that is not a node-admin, a Build whose Create names that path and name with another
    executable, which RDM replaces (§3)."
- **build-deployment.md §3:**
  - Old: "A new executable is a new node: a Build that adds it, `Create { nodes }` naming the
    executable, at a path and name no live node holds (build.md §12.5)."
  - New: "A new executable is a new node: a Build that adds it, `Create { nodes }` naming the
    executable at a path and name no live node holds, or naming a held path and name of a node that
    is not a node-admin with an executable other than its row records. RDM then replaces the node
    in the mesh's section, one path at a time: the node-type primary's role moves first when another
    node of its type is in the mesh, the held birth is deleted, and a node is created at the same
    path and name with the Build entry's executable, config and tags."
- **build-deployment.md §3:**
  - Old: "To keep the old node's data, the old node is stopped ... no page documents them together
    (Q345)."
  - New: "The new node takes the held birth's data dir when its own entry's `storage_persistence` is
    `persistent` and the directory exists; otherwise it gets a new one. A node-admin's executable is
    replaced by the primitives, each its own call."
- **build-deployment.md §4 and §6** ("a Delete of the birth, then a Create naming the executable"):
  - New: "a Create naming the executable at its path and name (§3)".
- **build.md §1**, the `Create` row's refusals ("a path and name already held"):
  - New: "a path and name already held, except a held node that is not a node-admin named with
    another executable, which is replaced (build-deployment.md §3)".
- **build.md §2 gate:**
  - Old: "A created node's path and name are free"
  - New: "A created node's path and name are free, or held by a node that is not a node-admin and
    named with another executable (a replacement, build-deployment.md §3)".
- **build.md §12.5:**
  - Old: "at a path and name no live node holds ... These are primitives, each its own call."
  - New: "at a path and name no live node holds, or at a held one with another executable, which RDM
    replaces (build-deployment.md §3); a node-admin's executable is replaced by the primitives, each
    its own call."
- **build.md line 117** ("delete, then create at the same path and name ... never a Build or part of
  one"): a repair stays as written; the sentence gains "a node's executable replacement is a Build's
  (build-deployment.md §3)".
- **node-create.md "Identity":**
  - Old: "a name free in it, checked at the Build's acceptance"
  - New: "a name free in it, or held by a node that is not a node-admin and replaced
    (build-deployment.md §3), checked at the Build's acceptance".
- **node-rpc-envelope.md** node.create row:
  - Old: "a held name answers NameHeld { by }"
  - New: "a held name answers NameHeld { by }, except a node that is not a node-admin named with
    another executable, which is replaced (build-deployment.md §3)".
- **states.md** ("A new executable is a new node with a new NodeId"): unchanged; the replacement is a
  new node with a new NodeId.
