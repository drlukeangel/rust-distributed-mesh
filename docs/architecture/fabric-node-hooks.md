# Fabric and node hooks

[Lifecycle index](fabric-mesh-node-lifecycles.md) · [States](lifecycles/states.md) · [Op contracts](node-rpc-envelope.md#the-op-ledger)

This page owns where application work enters an RDM lifecycle: the hooks, `BeforeReadyCtx`, each
hook's outcomes and the rafka-time handle.

RDM owns the lifecycle and its gates. The embedding application (Rafka) supplies the work
behind a gate through a hook. A hook is the only place application work enters an RDM
lifecycle. RDM never carries, decodes or interprets application data, and the application
never decides an RDM transition.

| Hook | Called by | When | Gates |
|---|---|---|---|
| [`before_ready`](#before_ready) | every node start | after accepted join and mesh-channel join; before topology installation | the node's ReadyForTraffic |
| [`after_ready`](#after_ready) | every node | after ReadyForTraffic | nothing |
| [`sync_state`](#sync_state) | fabric-primary | when the fabric enters state-sync | the fabric's state-commit |
| [`traffic_opened`](#traffic_opened) | fabric-primary | after the ready-for-traffic state is set | nothing; it is a notice |

RDM also exposes [rafka-time](#rafka-time) to the application as a read handle. That is not
a hook.

## before_ready

The app registers one async function before RDM starts:

```rust
async fn before_ready(ctx: BeforeReadyCtx) -> HookOutcome;

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

An admin answers that node.topology.get only once its own `before_ready` has returned
`Ok`. Until then it answers a typed `NotReady`, which the caller maps to
`Blocked { retry_on: PrimaryReady }`. A mesh's first node therefore never pulls from an
admin that holds nothing yet.

### Cancellation and lifecycle commands

Ending the in-flight start cancels the token. That covers node.connections.delete, node.stop and a provider
terminate. The hook and any pull it has in flight end, and no call is left behind. While a
start is Blocked, node.connections.delete, node.stop and Status stay admissible, so a birth that never
becomes ready can still be removed.

## after_ready

This hook is for optional work that does not gate readiness. Nothing required may run here,
and it is not a second path for projections.

## sync_state

When every mesh is ready-for-traffic, the fabric-primary moves the fabric to state-sync and
calls the application's `sync_state` hook. The application does the requested work: day 0
and seed, by script or by attaching a saved WAL image
([state-sync actions](lifecycles/fabric-state-sync.md#state-sync-actions)). The hook answers
the fabric-primary when the work ends.

The call and its answer carry the state as the
[envelope](node-rpc-envelope.md#application-state-sync-handoff) defines. The answer starts
[state-commit](lifecycles/fabric-state-commit.md), not ready-for-traffic;
[fabric state-sync](lifecycles/fabric-state-sync.md) owns the state's rules.

<a id="admit-attempt"></a>

**An older attempt's work (Q168).** The handoff and every application operation it sends carry
`build_id`, `submitted_at_ms` and `attempt`. The order is `(submitted_at_ms, attempt)`; a Build id
is never ordered. `submitted_at_ms` is the accepted Build's acceptance stamp, strictly increasing
across accepted Builds by construction: `max(now, newest accepted + 1)`, the newest read from the
builds store (R-BI3), so it holds across a restart and a clock rollback. A newer Build's attempt 1
supersedes an older Build's attempt 8.

Every node has `admit_attempt(build_id, submitted_at_ms, attempt)`, on the `NodeSeams` its served
ops are registered with (and on the running node; on a node-admin, its `Running`). It compares
against the node's durable admitted triple. An older pair is refused by name, `superseded`; a
different `build_id` carrying the same `submitted_at_ms` is refused by name, `ambiguous-order`; an
equal triple is admitted (a repeat). A newer triple is written durably beside the node's data dir
(`admitted-attempt`, one whole record) before `admit_attempt` returns, so its effect is never
allowed ahead of its record; it is read back when the node starts.

The application runs `admit_attempt` and the mutation under the same per-resource serialization
every competing attempt takes. Work prepared outside it (an async restore) takes the serialization
again and calls `admit_attempt` again immediately before publishing. Repeats of one attempt stay
subject to the operation's own idempotency. RDM owns the comparison and the durable value; the
application owns applying it at each mutation and publication.

## traffic_opened

After the ready-for-traffic state is set, the fabric-primary sends the application one
`traffic_opened` notice ([ready-for-traffic](lifecycles/fabric-ready-for-traffic.md)). Rafka
opens client traffic on that notice. The notice gates nothing in RDM.

## rafka-time

rafka-time is the fabric's one global time, kept by RDM; how a node adopts and disciplines it
is [origin-ordering.md §rafka-time](/home/admin/rafka-v2/docs/architecture/origin-ordering.md#rafka-time).

RDM exposes it to the application through one public, cloneable `RafkaTime` handle with
`now_ms()`. The app gets the handle from the running node returned by RDM's start, and from
`BeforeReadyCtx`. It is the same instance every RDM stamp reads; there is no second clock.
Rafka stamps its OrderKey `writer_timestamp_ms` from it.

## Observability

Each hook call is one span under the node's boot span, or under the fabric state's span for
`sync_state`. The span carries the outcome and the blocker or reason. `via-ready` follows the
successful `before_ready`. A cancelled hook names the cancellation.
