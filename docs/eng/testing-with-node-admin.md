# Testing with node-admin

**A cell acquires a fabric only through node-admin.** node-admin owns every node's lifecycle, the
fabric's topology and readiness; the cell owns its assertions. A cell never re-implements
convergence, never sleeps on a timer waiting for a node, and never manages gossip: it asks
node-admin over Node RPC, awaits the reply stream or the view, and asserts.

The harness this page uses is [testing-harnesses.md](testing-harnesses.md); the operations are the
lifecycle pages ([fabric-mesh-node-lifecycles.md](../architecture/fabric-mesh-node-lifecycles.md)).

## 1. Which node-admin

An estate's cell starts at its **entry** node-admin (`Estate::admin`, a `path.name`): first the
Day-0 admin the estate started, the fabric-primary of a new fabric
([fabric-create.md](../architecture/lifecycles/fabric-create.md)). Reads answer from that admin's
view; a Build call the entry does not own follows the redirect to the fabric-primary
(`crates/rdm-test/src/typed.rs`, module doc). `estate.enter(admin)` and
`estate.enter_fabric_primary()` change the entry.

Commands go down: the fabric-primary commands mesh-primaries, a mesh-primary commands its own
mesh's nodes, and a command for a mesh's node reaches it through its mesh-primary, never directly
(rafka-v2 CLAUDE.md "Commands go down"). A cell submits to node-admin and lets that chain run; it
never calls a node to make it change its own lifecycle status.

## 2. node-admin tells the truth about readiness

Readiness is three separate scopes, none a substitute for another
([states.md](../architecture/lifecycles/states.md);
[node-identity-and-state.md §5](../architecture/node-identity-and-state.md)):

| scope | lifecycle | who sets ready, on what |
|---|---|---|
| node | starting → pending → ready → connections-deleting → connections-deleted → stopping → stopped → deleting → deleted | its mesh-primary, on the node's `node.started` report after every start gate, `pre_node_ready` among them |
| mesh | pending → ready → deleting → deleted | the fabric-primary decides on the mesh-primary's report that every planned birth is ready; the mesh-primary records it |
| fabric | pending → ready | the fabric-primary, once every mesh the accepted Build names is ready and the attempt is claimed |

- There is exactly one readiness mechanism: the start's gates and the `node.started` report.
  Nothing times out into ready; a failed hook fails the start by name, a blocked one answers
  `Blocked` until its retry event (node-identity-and-state.md §5).
- `degraded` on a fabric or mesh is a health marker, not a lifecycle status (states.md).
- The views carry it: `FabricView`, `MeshView` and `NodeView`, with `ScopeStatus` and
  `NodeStatus` (`crates/rdm-client/src/lib.rs`).

**Waiting for it.** Every wait is bounded and names what it waited for:

| wait | what it reads |
|---|---|
| `estate.until_fabric_ready(within)` | the fabric's `ScopeStatus` reaching `Ready`; afterwards `estate.fabric_ready()` hands out the witness every kill takes |
| `estate.settles_to(&[(mesh, admins, rpc_nodes)], within)`, `settles_to_named(names, within)` | the node views until the fabric holds exactly that shape, every node ready |
| `estate.until_node(name, what, within, check)`, `until_view(what, within, check)` | one node's view, or all of them, until `check` holds |
| `Fabrics::fact(fact, within, holds)` | the fabric view until `holds` is true (`crates/rdm-client/src/objects.rs`) |
| `rdm_client::await_fact` | a typed fact, awaited under a bound |

A death during the fabric's first start is not tested: only an estate that has waited for its
fabric to reach ready hands out the `FabricReady` witness a kill takes (Q74;
`crates/rdm-test/src/estate.rs`, `FabricReady`).

## 3. Acquiring a fabric, option 1: a Build

A Build is the one way to change the fabric's shape: `build.create` with a complete-topology
request, `Change::Reconcile { based_on, nodes }`, every created path naming its executable
([build.md](../architecture/build.md); [build-deployment.md](../architecture/build-deployment.md)).

| harness call | what it submits |
|---|---|
| `estate.submit(&spec)` | a `BuildSpec` of counts per mesh, expanded against the current topology into the complete-topology request; the wire carries no count form (`crates/rdm-test/src/typed.rs`, `BuildSpec`) |
| `estate.submit_change(change)` | a `Change` the cell built itself |
| `estate.submit_named(what, nodes)` | the node specs the cell names, from the current topology and the estate's executables |
| `estate.shape_change(&desired)` | the `Change` that takes the fabric to a `FabricDesired` |

A count expands the way the compiler's count form resizes: a grow takes the lowest free ordinal of
the kind, a shrink drops the highest ordinal that is not the kind's primary
(`crates/rdm-test/src/executables.rs`, module doc).

The submit answers a `BuildStream`: the cell reads it to its terminal frame. `Builds::get(id)`
reattaches to a Build's receipts, `Builds::delete(id)` deletes its history
(`crates/rdm-client/src/objects.rs`). From outside a test binary,
`rdm-scenario <scenario.yaml>` brings a declared shape up through a Build
([testing-strategy-seed.md](testing-strategy-seed.md)).

## 4. Acquiring a fabric, option 2: node workflows

When a cell needs one operation on one node, it calls the workflow through `Nodes`
(`estate.nodes_obj()`; `crates/rdm-client/src/objects.rs`), each the operation its lifecycle page
names:

| call | operation |
|---|---|
| `nodes.create_named(based_on, specs)`, `estate.create_node(mesh, kind)` | [node.create](../architecture/lifecycles/node-create.md), itself a `build.create` of the complete topology (`Change::Reconcile`) |
| `nodes.stop(path)` / `nodes.start(path)` | [node.stop](../architecture/lifecycles/node-stop.md) / [node.start](../architecture/lifecycles/node-start.md): same node id and incarnation |
| `nodes.delete(path)` | [node.delete](../architecture/lifecycles/node-delete.md) |
| `nodes.delete_connections(path)`, `connections_delete(…)` | [node.connections.delete](../architecture/lifecycles/node-connections-delete.md) |
| `nodes.name_update(…)` | [node.name.update](../architecture/lifecycles/node-name-update.md) |
| `nodes.tags_create(…)`, `tags_delete(…)` | the node's tag rows |
| `nodes.primary_update(…)`, `fabric_primary_update(…)` | [node.primary.update, node.fabric-primary.update](../architecture/lifecycles/node-primary-update.md) |
| `nodes.steps(steps)` | Rafka's replace steps ([node-replace.md](../architecture/lifecycles/node-replace.md); `rdm_test::steps`) |
| `nodes.get(&filter)`, `Connections::get(birth, &filter)` | node.get, node.connection.get { node } |

Each answers a `WorkflowStream` (or a typed reply); the cell awaits it to its terminal and asserts
the typed outcome, never a string.

## 5. Tags

A node's tags come from its create spec (node-create.md "Spec"; build.md, the `tags` property).
`Nodes::get(&Filter::new().eq("tags", [label]))`, with `.eq("mesh", [mesh])` for one mesh's, reads
only the nodes carrying them (`node.get [tags: a]`, Q167;
[build-deployment.md](../architecture/build-deployment.md)): a cell reads its own nodes, never
another run's.

## 6. Faults

A fault is aimed at one node's runtime, after the fabric's first ready:

- `estate.kill_node(node)` sends SIGKILL to `node`'s runtime once `fabric_ready()` holds: the pid
  its data dir's `deployment.json` records (process provider) or its container's init (container
  provider) (`crates/rdm-test/src/estate.rs`, `pid_of`);
- the chaos kit ([chaos-kit.md](../architecture/chaos-kit.md)): `process_faults` kills, stops and
  continues one exact runtime read from its runtime record (a pid alone is never a target; a
  recycled pid is refused by name), `container_faults` acts on the exact containers of a
  container estate, and `netfault` cuts UDP between two sets of loopback ports.

The fabric-primary is never aimed at by the demo admin UI's Chaos tab
(`fabric-primary-is-never-a-target`, chaos-kit.md).

## 7. Where the executables come from

- Every created node's executable is the one its Build names, recorded on the row at the claim and
  re-hashed at the launch; a Build never names a provider
  ([build-deployment.md](../architecture/build-deployment.md); CLAUDE.md, `MESH_SPAWN_TYPE`).
- `RDM_EXECUTABLE_DIRS` lists the absolute host directories a Build's executables may come from;
  under the container provider each is mounted read-only, and an executable outside them is
  refused at acceptance (`executable-refused`) (CLAUDE.md env table).
- The harness names each path's executable from the test kit's own binaries, each hashed once per
  process, or from an explicit binding set (`crates/rdm-test/src/executables.rs`); the binaries are
  the test executable's own build (`estate.rs::bin_dir`, `RDM_BIN_DIR` to override).
- `estate.rs::binary_is_fresh` refuses a binary older than any source file in its dep-info,
  naming the file that changed and the rebuild command.

## 8. Division of responsibility

| node-admin owns | the cell asserts |
|---|---|
| launching every node, its readiness and its lifecycle states | the typed reply, refusal or stream terminal of the operation it submitted |
| the topology, gossip and the elections | the views node-admin answers, and the spans the operation emitted |
| node investigation and node recovery of an exited node | that the recovery happened, from its spans and the node's new birth |
| the estate's processes through the provider | that nothing the cell started outlived it |

## 9. Anti-patterns

- A fixed sleep to wait for a node → a bounded wait on the view (`until_node`, `until_view`,
  `until_fabric_ready`) that names what it waited for.
- Treating `pending` as ready → `ready` is the only ready status.
- Test-side retries of a command that did not converge → node-admin owns convergence; a command
  that never completes is a defect to read in its spans.
- Reading an internal map to decide readiness in an e2e cell → the views are the contract.
- Starting a node outside a Build or a node workflow → every node is created by node-admin.
- Calling a node directly to change its own lifecycle status → the command goes down through its primaries.
- A kill by a process-name pattern, or of a pid not read from the node's runtime → `kill_node` or
  the chaos kit's exact-runtime faults, after `FabricReady`.
- A cell that asserts when gossip arrives → wait for a property the gossip fact carries, never
  for its arrival time (rulings INDEX, Q90).
