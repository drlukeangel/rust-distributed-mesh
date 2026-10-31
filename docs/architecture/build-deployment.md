# Build deployment: the executable a node runs

[Lifecycle index](fabric-mesh-node-lifecycles.md) · [Build](build.md) · [Deployment provider](deployment-provider.md)

build.md owns the engine: the accepted topology, attempts, claims and receipts. deployment-provider.md
owns the provider: how one runtime is launched, inspected and terminated. This page owns what a Build
deploys: the executable each node runs, from the request to the row and back out through the reads.

## 1. The rule

A node's executable is an input of node.create and of nothing else. The birth runs it for its whole
life. A new executable is a new birth at the same `path.name` (Q157).

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
3. **Recorded at the claim.** The create's name claim records the executable on the node's row, the
   bootstrap admin's own included: it hashes the executable it is running. The row is the record of
   what this birth runs.
4. **Re-hashed at the launch.** The launching admin hashes the file before `DeployRuntime` and
   launches only on a match (deployment-provider.md). A missing file or a differing hash is refused by
   name; nothing is launched in its place.
5. **Reachable by the launcher.** A containerized admin mounts the executable directories its launch
   named (`RDM_EXECUTABLE_DIRS`), read-only at their host paths; an executable outside them is refused
   by name at acceptance, before anything destructive.

## 3. Changing a node's executable

Acceptance determines the desired topology. Each attempt computes its remaining work from the accepted
topology, the current node rows, observed state and its receipts (build.md §1). A live node whose row
already records its entry's config and executable is not deleted, created or restarted because another
node changes. A request that changes only the gateways' executables redeploys the gateways and leaves
every other node running as it was; an attempt interrupted halfway finishes the rest. Recovery of a
lost birth is node recovery's (§4).

A request that names a different executable for an existing path is a planned replacement. RDM
realizes it as node.delete then node.create at the path, a new node.id and incarnation, in its order
with its guards: a type's non-primary members before its primary, the primary's move first by
node.primary.update, the fabric-primary's move ending the caller's stream `FabricPrimaryMoved` and the
caller resubmitting the same build_id, a type's only node replaced with no update (node-primary-update.md
"No-successor cases"), and a mesh whose only node-admin is to be replaced first given a second node-admin
at a new `path.name` of that type on the new executable, Ready and mesh-primary, which stays. A path under node recovery is
replaced after that recovery's attempt completes, never in the same attempt. The successor's storage follows its create spec's `storage`: `Persistent` inherits the
predecessor's data dir and endpoint key, `Ephemeral` allocates a fresh one; its config and tags are its
Build entry's, written at its claim, never the predecessor's (node-replace.md "Properties"); a storage or placement change in the same request is the
same re-birth with the new properties. The row then records the new executable. node.stop and node.start keep the executable
(node-stop.md "Kept"); nothing changes a running birth's executable. A Build that puts the fabric
back to pending completes only after its own ready state is set (states.md).

## 4. Recovery

A heal is node investigation's verdict first (node-investigation.md). `reached`: the birth is commanded
back through node.mesh.join, the same process on the same executable; nothing is created and nothing
about its executable is read or written. A stop and a start keep the process and the executable
(node-stop.md "Kept"). Any other verdict re-creates the birth at its `path.name`, and that create
names no executable on its step, so it takes the row's (node-recovery.md "Retained"), never the
recovering admin's and never the topology's next entry. A heal never upgrades; a pending replacement
follows as its own step. A row written before the field
existed decodes with none, and a birth whose row records no executable has no automatic recovery: the
topology's entry is the target, not what the birth ran, so recovery of it is refused by name; the repair
is a request naming that path's executable, the ordinary replacement.

## 5. Reads

The node's row is the source. `node.get` returns every node row that matches its pairs, each with
its executable beside its status, config and tags (`NodeView.executable`): one node by `id`, a mesh's
nodes by `mesh`, the fabric's by no pair. The digest, the `MeshTopology` projection and `node.topology.get` carry no executable.

## 6. Refusals, by name

A created path with no executable; an executable outside the launcher's directories; a missing file;
a differing hash; node recovery of a birth whose row records no executable (repaired by a replacement
request naming it).
