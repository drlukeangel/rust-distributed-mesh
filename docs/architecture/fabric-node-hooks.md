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
| [`pre_node_ready`](#pre_node_ready) | every node | before the node reaches Ready: after the join and the mesh-channel join, before topology is written to the topology view | the node's Ready |
| [`pre_node_recreate`](#pre_node_recreate) | each mesh-primary running a node recovery | after the investigation's verdict, before node.delete and node.create of the recovered birth | the recovery's re-create |
| [`on_node_ready`](#on_node_ready) | every node | after its Ready | nothing; it is a notice |
| [`on_mesh_ready`](#on_mesh_ready) | each mesh-primary | after its mesh's ready is on its Mesh record | nothing; it is a notice |
| [`on_fabric_ready`](#on_fabric_ready) | fabric-primary | after the ready state is set | nothing; it is a notice |
| [`on_build_started`](#on_build_started) | fabric-primary | from the write of a Build's `rdm.build.started` event, at acceptance or the resubmission of a failed or incomplete Build | nothing; it is a notice |
| [`on_build_ended`](#on_build_ended) | fabric-primary | from the write of the Build's `rdm.build.ended` event: Complete, Incomplete or Failed | nothing; it is a notice |
| [`on_call_failed`](#on_call_failed) | every node | after one of its calls ends with its attempts spent | nothing; it is a notice |
| [`on_node_investigated`](#on_node_investigated) | each mesh-primary | after it records a node investigation's verdict on a node of its mesh | nothing; it is a notice |

`pre_` names a hook the application is asked before the act, held until it answers; `on_`
names a notice of what happened, fire and forget. Each notice is called once each time its scope
enters ready, or its Build ends, by the one that set it; a successor that adopts the state calls nothing.

RDM also exposes [rafka-time](#rafka-time) to the application as a read handle. That is not
a hook.

## pre_node_ready

The app registers one hook before RDM starts:

```rust
pub trait PreNodeReady: Send + Sync {
    /// The Send RDM runs this hook under: how many tries, and how long each may take.
    /// The application gives its own; with none, Send::default() (3 attempts, 10 s).
    fn send(&self) -> rdm_transport::Send { rdm_transport::Send::default() }

    /// Called before the node reaches Ready. ctx.cancel fires if the node is stopped or deleted meanwhile.
    async fn pre_node_ready(&self, ctx: &NodeReadyCtx) -> HookOutcome;
}

pub enum HookOutcome {
    Ok,                          // proceed: the node becomes Ready
    Pending { reason: String },  // not yet: this attempt ends, and RDM calls again on the hook's Send
    Failed { reason: String },   // stop now: the node's start ends Failed
}
```

There is one integration point, not a callback list. The hook's timing is its own code's `send()`,
next to the hook it governs; nothing for it goes on node.create or the type declaration (Q350).

### Where it runs

[Node start](lifecycles/node-start.md#end-to-end-sequence) step 3 calls the hook before the node
reaches Ready: after the join supplies the cert and rafka-time and the mesh channel is joined;
topology is written to the topology view only after the hook returns `Ok`. The hook subscribes the app's kind channels
through that channel and needs no topology aggregate. FirstStart and AfterStop run the same hook, and so
does a node its mesh-primary held `PendingReconnect` when it is heard again: commanded back through
node.mesh.join, it answers the tickle `Current { pending }` while its start runs (`Pending` while its
hook answers `Pending`), and is published ready only once the hook answers `Ok`. Every entry to
ready, from any other state, runs the hook; the process's data-dir inheritance is an independent fact
supplied by its spec.

### What the context carries

- the node: node id, incarnation, path and name;
- `start_kind`: FirstStart for create, AfterStop for a process rejoining, after a stop or when it
  is heard again after its mesh-primary held it `PendingReconnect`;
- whether the spec supplies an inherited data dir; start_kind alone does not decide it;
- the node-admin that answered the node's JoinNode, which it reads its kind channels from. It is
  `None`, with the reason, for the Day-0 root and for a fabric-primary recovered from its durable
  map. Those nodes have no node-admin to pull from and complete the hook from local state
  ([gossip.md §9](gossip.md#9-joinnode-op-0x1d--bootstrap-hydration));
- the Node RPC client the app uses for its own calls;
- the topology view handle, the topology the node holds;
- the channels handle (`Channels`), so the app joins its gossip kind channels (`Channels::join`,
  [gossip.md §8](gossip.md#8-entity-projection-gossip-remains-projection-not-source-of-truth)) before it takes a copy;
- the member cert issued at JoinNode ([fabric certs](fabric-certs.md));
- the [rafka-time](#rafka-time) handle;
- a cancellation token.

### What Rafka does inside it

1. Read the member cert from the context. RDM issued it at JoinNode with the `CertSigner` Rafka instantiated it with ([fabric certs](fabric-certs.md)).
2. Subscribe the entity-framework kind channels the node projects.
3. Read them from the node-admin that answered its JoinNode with [node.topology.get](lifecycles/node-topology-get.md)
   ([the pull at entry](gossip.md#8-entity-projection-gossip-remains-projection-not-source-of-truth)).
4. Apply the answer through the same receive path as live gossip, with each kind's snapshot
   completeness boundary as the floor. A row that arrives both by gossip and in the
   snapshot causes no duplicate state change or side effect.

### Outcomes and the gate

RDM runs the hook under its `Send` ([Node RPC §43](node-rpc.md#43-send-delivery-is-the-runtimes)):
each call must answer within the `TimeLimit`, and between calls RDM waits the one Send schedule
(cadence × 2^(n-1), ±25 % jitter, capped at the staleness floor).

| Answer | RDM does |
|---|---|
| `Ok` | start continues at [node start](lifecycles/node-start.md#end-to-end-sequence) step 4 and ends with node.started |
| `Pending { reason }` | that attempt ends; the node stays `Pending`, the reason is named in its blocker evidence and answered to the executor's ProbeNodeState as Status [`Pending { node_id, incarnation, reason }`](node-rpc-envelope.md#status-op-0x1b-rdm), and RDM calls the hook again after the backoff |
| no answer within the `TimeLimit` | the same as `Pending { reason: "time limit" }` |
| `Failed { reason }`, or the attempts spent | the start ends `Failed` with the hook step and the last reason, back to whoever started the node; no downstream readiness event is emitted |

A node never reaches Ready while its hook has not returned `Ok`. A start that ends `Failed` leaves the
node `Pending`, and nothing retries until someone acts:

| What started the node | Where the failure lands |
|---|---|
| a Build (Create) | the step's `rdm.build.step` event, `failed { reason }`; its mesh's section ends, and the Build ends `incomplete` (Q346) |
| a `node.start` command | the command ends `Failed { reason }` to its caller, recorded on the node record |
| [node recovery](lifecycles/node-recovery.md) | the recovery's step, `failed { reason }`, on its span |
| Day 0's first node-admin | its start ends `Failed`, and the Day 0 Build is `incomplete` |

For Build-backed work the executor frames the subject's `Pending` on the workflow stream from its
Status `Pending` reply, as the [Build carrier](node-rpc-envelope.md#build-op-0x20-rdm) defines, and
ends its `WaitForNodeReady` step failed from its Status `StartFailed { step: node.pre-node-ready, reason }`
reply: the node stays `Pending` and its process keeps running.

### The server side gates too

An admin answers that node.topology.get only once its own `pre_node_ready` has returned `Ok`.
Until then it answers a typed `NotReady`, and the caller's hook answers `Pending` with that reason.
RDM calls it again on its `Send`, so a mesh's first node never pulls from an admin that holds nothing
yet.

### Cancellation and lifecycle commands

Ending the in-flight start cancels the token. That covers node.connection.delete, node.stop and a
provider terminate. The hook and any pull it has in flight end, and no call is left behind. While a
start is `Pending`, node.connection.delete, node.stop and Status stay admissible, so a node that never
becomes ready can still be removed.

## pre_node_recreate

Once it writes a node investigation's verdict whose `next` is `recreate` ([node recovery](lifecycles/node-recovery.md) step 1), the mesh-primary running the recovery calls `pre_node_recreate` with the `NodeInvestigatedCtx` of [`on_node_investigated`](#on_node_investigated), before node.delete of the target birth and node.create at its path and name: nothing is created at that path and name until the application answers. The application releases what the lost birth held there (Rafka: a broker's departure close, its `write → none` rows durable, replica-routing.md §6) and answers `Ok`, and the recovery runs on, or `Failed { reason }`, and the recovery ends `failed { reason }`, named on its span, still owed: the next recovery tick runs it again, the hook included (Q344). There is no `Pending`: the application's work is one call under the hook's `Send`, whose time limit covers it. A `rejoin` calls no `pre_node_recreate`; an application that registers none is answered `Ok`.

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
`FabricReadyCtx { fabric_id, build_id, context }`: the accepted Build whose change made the fabric
ready and its observability context. Rafka opens client traffic
on that notice. The notice gates nothing in RDM.

## on_build_started

Each time the fabric-primary writes a Build's `rdm.build.started` event, at acceptance or when a caller's resubmission runs a failed or incomplete Build again, it sends the application one `on_build_started` notice from that write ([build.md §4.1](build.md#41-build-identity-is-active-reconciliation-state)), with `BuildStartedCtx { fabric_id, build_id, context }`: the Build and its observability context. A Build in progress that continues under a new fabric-primary or mesh-primary sends none. Each `on_build_started` is followed by one `on_build_ended` for the same run. The notice is sent at most once per opening, and `build.get` is the durable read. The notice gates nothing in RDM.

## on_build_ended

Each time a Build's terminal state is recorded, the fabric-primary sends the application one `on_build_ended` notice, the ending's last step ([build.md §4.1](build.md#41-build-identity-is-active-reconciliation-state)), from the write of the Build's `rdm.build.ended` event, with `BuildEndedCtx { fabric_id, build_id, state, context }`: the Build, its state, `Complete`, `Incomplete { sections }` or `Failed { reason, code }` as its caller reads it on the Build stream (R-BI4), and its observability context. A Build that put the fabric back to pending completes when its ready state is set, so its `Complete` notice follows its `on_fabric_ready`. The notice is sent at most once per ending: a fabric-primary lost before it sends it leaves none, and `build.get` is the durable read. The notice gates nothing in RDM.

## on_call_failed

When a call this node opened ends with its attempts spent ([node-rpc.md §43](node-rpc.md#43-send-delivery-is-the-runtimes)), RDM calls `on_call_failed` once, with `CallFailedCtx { sender, receiver, reason }`, on a task of its own, after the call has ended to its owner. `sender` and `receiver` are each the birth's `(node_id, incarnation, path, name)`, `receiver` none for a call that names no birth; `reason` is the call's stored end, `NotSent` with its reason (nothing reached the receiver) or `Indeterminate` with its reason (the request crossed the commit cut and no answer came). The notice gates nothing: the call's owner already has its outcome by name; the application acts by its own rule (ask another node, mark a route) and owns any duplicate boundary a later resend of its own crosses. It is not a verdict on the receiver: that is [`on_node_investigated`](#on_node_investigated), once the primary above the birth has investigated it.

## on_node_investigated

Once it writes a node investigation's verdict on a node of its mesh as its `rdm.node.investigated` event ([build.md §5](build.md#5-buildstateadapter); [node investigation](lifecycles/node-investigation.md)), the mesh-primary calls `on_node_investigated` once, with `NodeInvestigatedCtx { fabric_id, mesh_id, node_id, incarnation, path, name, verdict, next, context }`, on a task of its own. `verdict` is the investigation's; `next` is what RDM does with the birth: `rejoin` (commanded back through node.mesh.join) or `recreate` (re-created at the same path and name with a new node.id). The notice gates nothing in RDM and orders nothing: a re-create may be under way when the application's hook runs. Work that must come before the re-create belongs in [`pre_node_recreate`](#pre_node_recreate).

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
`on_fabric_ready`, under the Build's span for `on_build_started` and `on_build_ended` (`rdm.node_admin.build.update.via-on-build-started`, `rdm.node_admin.build.update.via-on-build-ended`), or of its own for `on_mesh_ready` (`rdm.node_admin.mesh.update.via-on-mesh-ready`), under the spent call's span for `on_call_failed` (`rdm.mesh.node.update.via-on-call-failed`), under the span that records the verdict for `on_node_investigated` (`rdm.node_admin.node.update.via-on-node-investigated`), under the recovery's span for `pre_node_recreate` (`rdm.node_admin.node.update.via-pre-node-recreate`). The span carries the outcome and the blocker or reason. `via-ready` follows the
successful `pre_node_ready`. A cancelled hook names the cancellation.
