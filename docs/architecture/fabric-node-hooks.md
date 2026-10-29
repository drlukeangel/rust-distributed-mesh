# Fabric and node hooks

[Lifecycle index](fabric-mesh-node-lifecycles.md) · [States](lifecycles/states.md) · [Op contracts](node-rpc-envelope.md#the-op-ledger)

This page owns where application work enters an RDM lifecycle: the hooks, their contexts
(`NodeReadyCtx`, `MeshReadyCtx`, `FabricReadyCtx`, `BuildStartedCtx`, `BuildEndedCtx`), each hook's outcomes and the rafka-time handle.

RDM owns the lifecycle and its gates. The embedding application (Rafka) supplies the work
behind a gate through a hook. A hook is the only place application work enters an RDM
lifecycle. RDM never carries, decodes or interprets application data, and the application
never decides an RDM transition.

| Hook | Called by | When | Gates |
|---|---|---|---|
| [`pre_node_ready`](#pre_node_ready) | every node start | after accepted join and mesh-channel join; before topology installation | the node's Ready |
| [`on_node_ready`](#on_node_ready) | every node | after its Ready | nothing; it is a notice |
| [`on_mesh_ready`](#on_mesh_ready) | each mesh-primary | after its mesh's ready is on its Mesh record | nothing; it is a notice |
| [`on_fabric_ready`](#on_fabric_ready) | fabric-primary | after the ready state is set | nothing; it is a notice |
| [`on_build_started`](#on_build_started) | fabric-primary | after the claim of a Build's first attempt, or of the attempt that reopens an ended Build, is recorded | nothing; it is a notice |
| [`on_build_ended`](#on_build_ended) | fabric-primary | after the Build's Complete or Failed is recorded | nothing; it is a notice |

`pre_` names the one hook the application is asked before the act and holds until it answers; `on_`
names a notice of what happened, fire and forget. Each notice is called once each time its scope
enters ready, or its Build ends, by the one that set it; a successor that adopts the state calls nothing.

RDM also exposes [rafka-time](#rafka-time) to the application as a read handle. That is not
a hook.

## pre_node_ready

The app registers one async function before RDM starts:

```rust
async fn pre_node_ready(ctx: NodeReadyCtx) -> HookOutcome;

enum HookOutcome {
    Ok,
    Blocked { reason: String, retry_on: RetryOn },
    Failed { reason: String },
}
```

There is one integration point, not a callback list.

### Where it runs

[Node start](lifecycles/node-start.md#end-to-end-sequence) step 3 calls the hook after the
accepted join supplies the cert and rafka-time and the mesh channel is joined; topology is
installed only after the hook returns Ok. The hook subscribes the app's kind topics through that
channel and needs no topology snapshot. FirstStart and AfterStop run the same hook, and so does a birth its mesh-primary held
`PendingReconnect` when it is heard again: commanded back through node.mesh.join, it answers the
tickle `Current { pending }` while its start runs (`Blocked` only while its hook reports a blocker),
with the cert its accepting primary issues at the join and rafka-time refreshed last, and is
published ready only once the hook passes. Every entry to ready, from any other state, runs the hook;
the process's data-dir inheritance is an independent fact supplied by its spec.

### What the context carries

- the exact birth: node id, incarnation and path.name;
- `start_kind`: FirstStart for create, AfterStop for a process rejoining, after a stop or when it
  is heard again after its mesh-primary held it `PendingReconnect`;
- whether the spec supplies an inherited data dir; start_kind alone does not decide it;
- the accepting primary, i.e. the admin that accepted the JoinNode. It is `None`, with the
  reason, for the Day-0 root and for a fabric-primary recovered from its durable map. Those
  births have no primary to pull from and complete the hook from local state
  ([gossip.md §9](gossip.md#9-joinnode-op-0x1d--bootstrap-hydration)).
- the Node RPC client the app uses for its own calls;
- the topology view handle, so the app subscribes its gossip kind topics before it takes a
  snapshot;
- the member cert issued at JoinNode ([fabric certs](fabric-certs.md));
- the [rafka-time](#rafka-time) handle;
- a cancellation token.

### What Rafka does inside it

1. Read the member cert from the context. RDM issued it at JoinNode with the `CertSigner` Rafka instantiated it with ([fabric certs](fabric-certs.md)).
2. Subscribe the entity-framework kind topics the node projects.
3. Read them from the accepting primary with [node.topology.get](lifecycles/node-topology-get.md)
   ([the pull at entry](gossip.md#8-entity-projection-gossip-remains-projection-not-source-of-truth)).
4. Apply the answer through the same receive path as live gossip, with each kind's snapshot
   completeness boundary as the floor. A row that arrives both by gossip and in the
   snapshot causes no duplicate state change or side effect.

### Outcomes and the gate

| Outcome | RDM does |
|---|---|
| `Ok` | start continues at [node start](lifecycles/node-start.md#end-to-end-sequence) step 4 and ends with node.started |
| `Blocked { reason, retry_on }` | readiness remains blocked; the reason is named in the blocker evidence and answered to the executor's ProbeNodeState as Status [`Blocked { node_id, incarnation, reason, retry_on }`](node-rpc-envelope.md#status-op-0x1b-rdm); the hook runs again only when retry_on occurs |
| `Failed { reason }` | the start stream ends Failed with the hook step and reason; no downstream readiness event is emitted |

`retry_on` is the typed `RetryOn` the [envelope](node-rpc-envelope.md#status-op-0x1b-rdm) defines.
A retry is level-triggered: RDM arms the
wait, then checks the condition again, so an event that fired before arming cannot leave the
node waiting. RDM never retries on a timer.

A node never reaches Ready while its hook has not returned `Ok`.

For Build-backed work the executor frames the blocker on the workflow stream from the
subject's Status `Blocked` reply, as the [Build carrier](node-rpc-envelope.md#build-op-0x20-rdm)
defines.

### The server side gates too

An admin answers that node.topology.get only once its own `pre_node_ready` has returned
`Ok`. Until then it answers a typed `NotReady`, which the caller maps to
`Blocked { retry_on: PrimaryReady }`. A mesh's first node therefore never pulls from an
admin that holds nothing yet.

### Cancellation and lifecycle commands

Ending the in-flight start cancels the token. That covers node.connections.delete, node.stop and a provider
terminate. The hook and any pull it has in flight end, and no call is left behind. While a
start is Blocked, node.connections.delete, node.stop and Status stay admissible, so a birth that never
becomes ready can still be removed.

## on_node_ready

After the node reports node.started, RDM calls `on_node_ready` once, with the node's `NodeReadyCtx`,
on a task of its own. It is for optional work that does not gate readiness. Nothing required may run
here, and it is not a second path for projections.

## on_mesh_ready

Once its mesh's ready is on its Mesh record ([states](lifecycles/states.md#who-sets-each-state),
Q144), the mesh-primary calls `on_mesh_ready` once, with `MeshReadyCtx { fabric_id, mesh_id, mesh }`,
on a task of its own. The notice gates nothing in RDM.

## on_fabric_ready

After the ready state is set, the fabric-primary sends the application one
`on_fabric_ready` notice ([ready](lifecycles/states.md#the-fabrics-ready)), with
`FabricReadyCtx { fabric_id, build_id, attempt, context }`: the accepted Build and attempt whose change
made the fabric ready and that attempt's observability context. Rafka opens client traffic
on that notice. The notice gates nothing in RDM.

## on_build_started

Each time a Build's first attempt is claimed, or an attempt that reopens a Build that has ended is claimed, the fabric-primary sends the application one `on_build_started` notice, the opening's first step ([build.md §4.1](build.md#41-build-identity-is-active-reconciliation-state)), with `BuildStartedCtx { fabric_id, build_id, attempt, reason, context }`: the Build, the attempt and its reason (`requested` at acceptance, or the reopening attempt's: a drift repair, a stop, a start), and that attempt's observability context. An attempt that continues a Build in progress (a takeover, or the next attempt after a hand-back) sends none. Each `on_build_started` is followed by one `on_build_ended` for the same opening. The notice is sent at most once per opening, and `build.get` is the durable read. The notice gates nothing in RDM.

## on_build_ended

Each time a Build's terminal state is recorded, the fabric-primary sends the application one `on_build_ended` notice, the ending's last step ([build.md §4.1](build.md#41-build-identity-is-active-reconciliation-state)), with `BuildEndedCtx { fabric_id, build_id, attempt, reason, state, context }`: the Build, the attempt that ended it and that attempt's reason (`requested`, or a later attempt's: a drift repair, a stop, a takeover), its state, `Complete` or `Failed { step, reason, code }` as its caller reads it on the Build stream (R-BI4), and that attempt's observability context. A Build that put the fabric back to pending completes when its ready state is set, so its `Complete` notice follows its `on_fabric_ready`. The notice is sent at most once per ending: a fabric-primary lost before it sends it leaves none, and `build.get` is the durable read. The notice gates nothing in RDM.

## rafka-time

rafka-time is the fabric's one global time, kept by RDM. Every node's rafka-time descends from the
fabric-primary's:

- **The fabric's root adopts its own clock, once.** The Day-0 root, a node-admin with no launcher,
  no seeds and no recovery flag, adopts its own OS clock as rafka-time at boot; so does the
  node-admin a [fabric recovery](lifecycles/fabric-recovery.md#nothing-alive) launches when every
  node the store names is confirmed gone by the provider. No node serves rafka-time to either.
- **Every other process adopts from the answer to its pull:** the JoinNode answer, its start's last
  step (a `GetTopology` read), a re-entry's read, and a mesh-primary's pull from a new
  fabric-primary when the fabric-primary changes. A node adopts from whichever node answers; a
  mesh-primary adopts only from the fabric-primary, refuses any other answer by name and keeps the
  rafka-time it holds. A change of fabric-primary changes the node that serves rafka-time, not the
  clock. Neither side estimates network delay: the value is adopted as the answering node read it.
- **A node answers a pull with the rafka-time it adopted**, never its own clock.
- **Between pulls a node keeps its rafka-time on its primary's** (a node's mesh-primary; a
  mesh-primary's, the fabric-primary) from the stamps that primary already publishes, and from
  nothing else: a stamp ahead of the clock steps it forward at once; a clock whose least-delayed
  sample of a window is behind by more than the jitter allowance has run fast and slows until the
  excess is gone. A read answers `max(reference + elapsed, last returned)`, so rafka-time never
  steps back, and the OS clock takes no part in a read.

RDM exposes it to the application through one public, cloneable `RafkaTime` handle with
`now_ms()`. The app gets the handle from the running node returned by RDM's start, and from
`NodeReadyCtx`. It is the same instance every RDM stamp reads; there is no second clock.

## Observability

Each hook call is one span under the node's boot span, under the fabric's change span for
`on_fabric_ready`, under the attempt's span for `on_build_started` and `on_build_ended` (`rdm.node_admin.build.update.via-on-build-started`, `rdm.node_admin.build.update.via-on-build-ended`), or of its own for `on_mesh_ready` (`rdm.node_admin.mesh.update.via-on-mesh-ready`). The span carries the outcome and the blocker or reason. `via-ready` follows the
successful `pre_node_ready`. A cancelled hook names the cancellation.
