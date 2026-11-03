# Build deployment: the executable a node runs

[Lifecycle index](fabric-mesh-node-lifecycles.md) · [Build](build.md) · [Deployment provider](deployment-provider.md)

build.md owns the engine: the accepted topology, the Build record and its events. deployment-provider.md
owns the provider: how one runtime is launched, inspected and terminated. This page owns what a Build
deploys: the executable each node runs, from the request to the row and back out through the reads.

## 1. The rule

A node's executable is an input of node.create and of nothing else: the executable used to launch the
node. The birth runs it for its whole life. A new executable is a new node: a Build that adds it
(Q345).

## 2. From the request to the row

1. **Named per node.** A Build request that creates nodes names each node's create spec: the node's
   own properties, flat (Q166). The spec carries the executable: its path, its sha256 and, under the
   container provider, the image. There is no per-type default in RDM and no stored default anywhere.
   A created path whose spec names no executable is refused by name at acceptance and nothing runs. A product's door may expand a
   per-type input into per-path entries before the typed call; RDM never sees a type-level default.
2. **Compiled at acceptance.** The fabric-primary compiles the request against the current topology.
   Every created path's entry in the accepted topology holds its executable, and the request is never
   re-read, as counts are never re-read (build.md §1). The accepted topology is the one durable answer
   to "what should this node run", beside the shape.
3. **Recorded at the create.** The create's row write records the executable on the node's row, the
   bootstrap admin's own included: it hashes the executable it is running. The row is the record of
   what this birth runs.
4. **Re-hashed at the launch.** The launching admin hashes the file before `DeployRuntime` and
   launches only on a match (deployment-provider.md). A missing file or a differing hash is refused by
   name; nothing is launched in its place.
5. **Reachable by the launcher.** A containerized admin mounts the executable directories its launch
   named (`RDM_EXECUTABLE_DIRS`), read-only at their host paths; an executable outside them is refused
   by name at acceptance, before anything destructive.

## 3. A new executable is a new node

A node's executable never changes: its row records the executable used to launch it. A new executable
is a new node: a Build that adds it, `Create { nodes }` naming the executable, at a path and name no
live node holds (build.md §12.5). A live node whose row records its executable is not deleted, created
or restarted because another node changes. Recovery of a lost birth is node recovery's (§4).

To keep the old node's data, the old node is stopped ([node.stop](lifecycles/node-stop.md)) and its
name changed with [node.name.update](lifecycles/node-name-update.md) first; the Create then takes the
path and name it released, and the old node stays stopped with its data until a Delete removes it.
Without the old node's data, a Delete of the old node releases its path and name for the Create.
These are primitives, each its own call; no page documents them together (Q345). Going back is a Build
of the former topology and configuration: nothing is ever reverted. The new node's config and tags are
its Build entry's, never the old node's. node.stop and node.start keep the executable (node-stop.md
"Kept"); nothing changes a running birth's executable. A Build that puts the fabric back to pending
completes only after its own ready state is set (states.md).

## 4. Recovery

A heal is node investigation's verdict first (node-investigation.md). `reached`: the birth is commanded
back through node.mesh.join, the same process on the same executable; nothing is created and nothing
about its executable is read or written. A stop and a start keep the process and the executable
(node-stop.md "Kept"). Any other verdict re-creates the birth at its `path` and `name`, and that create
names no executable on its step, so it takes the row's (node-recovery.md "Retained"), never the
recovering admin's and never the topology's next entry. A heal never upgrades. A row written before the field
existed decodes with none, and a birth whose row records no executable has no automatic recovery: the
topology's entry is the target, not what the birth ran, so recovery of it is refused by name; the repair
is a new node: a Delete of the birth, then a Create naming the executable (§3).

## 5. Reads

The node's row is the source. `node.get` returns every node row that matches its pairs, each with
its executable beside its status, config and tags (`NodeView.executable`): one node by `id`, a mesh's
nodes by `mesh`, the fabric's by no pair. The digest, the `MeshTopology` projection and `node.topology.get` carry no executable.

## 6. Refusals, by name

A created path with no executable; an executable outside the launcher's directories; a missing file;
a differing hash; node recovery of a birth whose row records no executable (repaired by a Delete and a
Create naming the executable, §3).
