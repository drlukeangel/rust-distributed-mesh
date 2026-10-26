# Fabric and node hooks

[Lifecycle index](fabric-mesh-node-lifecycles.md) · [States](lifecycles/states.md) · [Op contracts](node-rpc-envelope.md#the-op-ledger)

This page owns where application work enters an RDM lifecycle: the hooks, their contexts
(`NodeReadyCtx`, `MeshReadyCtx`, `FabricReadyCtx`), each hook's outcomes and the rafka-time handle.

RDM owns the lifecycle and its gates. The embedding application (Rafka) supplies the work
behind a gate through a hook. A hook is the only place application work enters an RDM
lifecycle. RDM never carries, decodes or interprets application data, and the application
never decides an RDM transition.

| Hook | Called by | When | Gates |
|---|---|---|---|
| [`pre_node_ready`](#pre_node_ready) | every node start | after accepted join and mesh-channel join; before topology installation | the node's ReadyForTraffic |
| [`on_node_ready`](#on_node_ready) | every node | after its ReadyForTraffic | nothing; it is a notice |
| [`on_mesh_ready`](#on_mesh_ready) | each mesh-primary | after its mesh's ready-for-traffic is on its Mesh record | nothing; it is a notice |
| [`on_fabric_ready`](#on_fabric_ready) | fabric-primary | after the ready-for-traffic state is set | nothing; it is a notice |

`pre_` names the one hook the application is asked before the act and holds until it answers; `on_`
names a notice of what happened, fire and forget. Each notice is called once each time its scope
enters ready-for-traffic, by the one that set it; a successor that adopts the state calls nothing.

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
published ready-for-traffic only once the hook passes. Every entry to ready-for-traffic, from any other state, runs the hook;
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

A node never reaches ReadyForTraffic while its hook has not returned `Ok`.

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

Once its mesh's ready-for-traffic is on its Mesh record ([states](lifecycles/states.md#who-sets-each-state),
Q144), the mesh-primary calls `on_mesh_ready` once, with `MeshReadyCtx { fabric_id, mesh_id, mesh }`,
on a task of its own. The notice gates nothing in RDM.

## on_fabric_ready

After the ready-for-traffic state is set, the fabric-primary sends the application one
`on_fabric_ready` notice ([ready-for-traffic](lifecycles/fabric-ready-for-traffic.md)), with
`FabricReadyCtx { fabric_id, build_id, attempt, context }`: the accepted Build and attempt whose change
made the fabric ready-for-traffic and that attempt's observability context. Rafka opens client traffic
on that notice. The notice gates nothing in RDM.

## rafka-time

rafka-time is the fabric's one global time, kept by RDM; how a node adopts and disciplines it
is [origin-ordering.md §rafka-time](/home/admin/rafka-v2/docs/architecture/origin-ordering.md#rafka-time).

RDM exposes it to the application through one public, cloneable `RafkaTime` handle with
`now_ms()`. The app gets the handle from the running node returned by RDM's start, and from
`NodeReadyCtx`. It is the same instance every RDM stamp reads; there is no second clock.
Rafka stamps its OrderKey `writer_timestamp_ms` from it.

## Observability

Each hook call is one span under the node's boot span, under the fabric's change span for
`on_fabric_ready`, or of its own for `on_mesh_ready` (`rdm.node_admin.mesh.update.via-on-mesh-ready`). The span carries the outcome and the blocker or reason. `via-ready` follows the
successful `pre_node_ready`. A cancelled hook names the cancellation.
