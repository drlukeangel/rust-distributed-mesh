# Node recovery

[Lifecycle index](../fabric-mesh-node-lifecycles.md) · [States](states.md) · [Typed calls and wire mapping](../node-rpc-envelope.md#typed-object-contract) · [node investigation](node-investigation.md)

Node recovery is the primary's own workflow for a member that goes unheard outside any workflow: a
member that [node investigation](node-investigation.md) did not reach is re-created with its path
and name and a new node.id. It is never a Build or part of one (Q342). The mesh-primary runs it for its own members,
whatever their type: it tracks them, runs the investigation and runs the recovery. The app takes part only through the start hook of the new node.
Recovery is not a caller op. (A mesh none of whose node-admins can be reached is this page's case
with no successor named: the fabric-primary investigates the lost mesh-primary and re-creates it at
its path and name, Q335.)

## Operation summary

| Field | Contract |
|---|---|
| Runs it | The mesh-primary of the member's mesh; for a lost mesh-primary, the successor its election names; the fabric-primary only when no successor takes the role (Q335) |
| Detection | The mesh-primary's tick (every gossip interval) reads the members unheard past the staleness floor, which it writes `PendingReconnect` as it starts their self-heal, and the members an ordered investigation names: a command to them never sent for a reachability reason, or a call to them ended with its attempts spent (step 1) |
| Investigation | [node investigation](node-investigation.md): the by-destination connections index, node.runtime.inspect, node.tickle direct and via peers; one verdict |
| Trigger | The verdict and the investigation's reason (Q111, Q393): reached, commanded back through node.mesh.join; any other verdict as step 3 has it. Nothing waits on the provider's `Exited` |
| Execution | The mesh-primary runs the recovery itself and records each step on the node record; the investigation's verdict is its `rdm.node.investigated` event ([build.md §5](../build.md#5-buildstateadapter)). It is never a Build: no build.create, no Build record, nothing claimed. Work on one node is serialized at its mesh-primary: a recovery and any other step on that node (a node command, a Build's step) run one after the other, and a Build in flight elsewhere in the mesh neither holds it nor is held by it. A lost mesh-primary is investigated and recovered by the successor its election names, as any member of its mesh, its own workflow; the fabric-primary investigates and recovers it only when no successor takes the role (Q335) |
| Steps | node.delete of the unheard instance, node.create at the same path and name, keyed on the target birth `(node.id, incarnation)` |
| Retained | path and name, placement and storage (from the node record), config and tags (inherited from the deleted row) and the executable (seeded from the deleted row: a recovered node runs the bin its row records, never the recovering admin's) ; a row written before the executable field decodes with none, and node recovery of that birth is refused by name: the topology's entry is the target, not what the birth ran; the repair is a Build that deletes the node and creates its path again naming its executable: a new executable is a new node ([build-deployment.md](../build-deployment.md), Q345); the predecessor's data dir when the verdict is `exited`, its `storage_persistence` is `persistent` and the directory exists |
| New | node.id, incarnation, `endpoint_id` and `transport_addr` (an OS-assigned port) on every verdict (Q358), exact runtime; a newly allocated data dir on any verdict but `exited`, with `ephemeral` storage, and when the predecessor's directory does not exist (Q359) |
| App part | The new node's start runs [`pre_node_ready`](../fabric-node-hooks.md#pre_node_ready) with start_kind FirstStart and `inherited = true` over a kept data dir, or `inherited = false` over a newly allocated one, then its [`on_node_ready`](../fabric-node-hooks.md#on_node_ready) notice after node.started |
| Lineage | No supersedes: recovery creates a new node.id ([node identity](../node-identity-and-state.md#2-a-node-is-an-entity-and-it-has-three-ids)) |

## Detection and the verdict

The mesh-primary runs one loop for its own members on every gossip interval, never on a timer of its
own. A recovery that ends failed is run again by the next check that finds the node still lost; its calls retry only under their `Send` (Q344).

1. **Silence.** Each node's pruner holds a member silent once it is unheard past the staleness floor; its mesh-primary writes it `PendingReconnect` as it starts the self-heal. Nothing is sent; the mark comes from the digests not arriving. Peers stop routing
   new work to it. Silence is not death. Every node-admin keeps the silence bookkeeping
   (`first_seen`), so a node-admin that becomes mesh-primary mid-window continues the window already
   running. A command to it never sent for a reachability reason (the connect refused, timed out or failed; the endpoint unresolved), or a call to it that ends with its attempts spent (its own, or one another caller reports with Status `ReportCallFailed`), starts an ordered investigation of it, which sets no `PendingReconnect` (step 3).
   `PendingReconnect` never replaces a lifecycle state the node reported and the mesh-primary recorded (`connections-deleting`, `stopping`, `stopped`); it ends when a digest of the exact birth arrives after it, when the birth answers its tickle, or when the view no longer holds that birth. A birth heard again runs its `pre_node_ready` hook again before it is
   ready ([fabric-node-hooks.md](../fabric-node-hooks.md#where-it-runs)).
2. **Investigation.** Only the mesh-primary acts. Half a floor after the mark it runs
   [node investigation](node-investigation.md) on the exact birth: the by-destination connections index, node.runtime.inspect (`exited` or `os-stopped` is the verdict, and no tickle is sent), node.tickle direct, the same tickle via up to two eligible live peers of the birth's mesh sent at once. Any answer from the exact birth ends the ladder and cancels the
   probes still running; one failed peer probe does not cancel the other; every probe names the same
   exact `(node.id, incarnation)`. For a lost mesh-primary the successor its election names runs it as for
   any member ([elections §4.1](../fabric-node-lifecycle-elections.md#41-the-nodes-of-one-type-in-one-mesh); [mesh recovery](mesh-recovery.md));
   the fabric-primary runs it, without the half-floor wait, only when the mesh's aggregate stays unheard for twice the staleness floor (60 s at the default floor): no successor took the role in the floor after the election, which is the mesh's own node-admin's to act in (Q335, Q349).
3. **The verdict is the one decision (Q111).** `reached`: a held birth that answered, or that is heard
   again by its digest, is commanded back through [node.mesh.join](node-mesh-join.md) by the primary
   above it (its mesh-primary; the fabric-primary for a mesh-primary: every admin looks, one commands)
   and runs its start sequence again before it is ready (Q87). A lost node-admin that held a primary role takes none back: commanded back, it unmarks itself as primary before it is ready and takes the mesh's primary from the successor's record; re-created, its new birth starts with no primary role. The command goes only to a
   birth the view holds off ready (the topology decides it, not what an admin just started
   has heard), once per silence (not again within a staleness floor, nor until it is heard
   ready or unheard again), never to the fabric-primary, which has no primary above it, and
   never to a birth with a command of its primary open or in flight, which is that command's. A
   commanded start that stops at a step leaves the birth off its channels, Pending, for the next
   command. Any other verdict follows the investigation's reason (Q393). A self-heal (the birth unheard past
   the staleness floor, its mesh-primary investigating it, the birth `PendingReconnect` while it runs):
   `running` is commanded back through node.mesh.join, except `running` from an investigator that hears no other node, which is unreachable from it, not lost: nothing is written, and it investigates again (Q153), and `os-stopped` commanded to start (node.start; a start it refuses as already live is `running`), each sent until answered; `exited`, a birth not proven by the end of
   the retries, or a join or start unanswered at the end of its attempts is written `dead`, and the
   recovery below runs at once. An ordered investigation (a command to it never sent for a reachability reason, another caller's `ReportCallFailed`, the mesh-primary's own call ended with its attempts spent) sets no `PendingReconnect`: a verdict that matches the birth's status in the topology does nothing (`running` and a live status, or a lifecycle state the node reported; `os-stopped` and `stopped`; `exited` and `dead`); one that differs follows the self-heal. Nothing waits for the provider's `Exited`
   (Q110, Q111). A birth that comes back once its verdict is not `reached`, during its successor's creation or after it, is a deleted birth: every view refuses
   its Digest by name ([gossip.md receiver rules](../gossip.md#31-frame-catalog)), and its mesh-primary
   decides it by its path and name and its atlas ([node investigation](node-investigation.md), Q207): its
   recovery or its re-created birth is there, so the primary whose atlas decided it terminates its runtime.

## Numbered end-to-end sequence

1. The investigation ends in a verdict other than `reached`, and the mesh-primary (for a lost mesh-primary, the successor its election names; the fabric-primary only when no successor takes the role) runs the recovery of the target birth `(node.id, incarnation)` itself,
   recording each step on the node record. A birth already recovered or deleted is not recovered again: never a second recovery. Another step on the same node runs
   before or after it at the mesh-primary, never beside it.
2. [node.delete](node-delete.md#numbered-end-to-end-sequence) of the target instance, as a target with
   no RPC to the old process and no stop frames. Its first step is
   `TerminateRuntime { node_id, incarnation, handle }` on the exact runtime handle recorded at launch,
   read at the step's instant and never skipped for what it reads, except `os-stopped`, which recovery
   leaves as it is ([node.delete](node-delete.md)). A `running` runtime is the provider's
   `terminate(handle, Graceful)` then `Forced` after the stop grace: Q110's "then you kill it". An
   `exited` runtime is the same call: the provider removes what the runtime left (a container and the
   address it held, its logs kept beside the data dir first; a process leaves nothing), so no runtime of
   a superseded birth stays on the host; a runtime already removed terminates as done (Q145). A
   terminate the provider refuses ends the recovery failed at `TerminateRuntime` by name, recorded on the
   node record; the next tick that finds the node still lost decides again.
3. [node.create](node-create.md) at the same path and name, its spec naming the predecessor's data dir when the verdict was `exited`, its `storage_persistence` is `persistent` and the directory exists (a newly allocated data dir otherwise): it mints a new endpoint key and writes it there as the new node's own, never reading the predecessor's, and binds port 0, with a new node.id, incarnation, `endpoint_id` and `transport_addr` (Q358). Its config is seeded from the deleted row at that path and name.
4. Create runs the whole [start](node-start.md#end-to-end-sequence) with FirstStart and
   `inherited = true` over the kept data dir (or `inherited = false` over a newly allocated one), and
   ends node.created.

## Command, completion and gossip messages

```text
mesh-primary tick: PendingReconnect → node investigation → verdict
  reached:    node.mesh.join (commanded back)
  otherwise:  the mesh-primary runs the recovery
                node.delete(unheard instance) → node.deleted   (TerminateRuntime first; no stop frames)
                node.create(same path and name; the predecessor's data dir when exited and persistent) → every FirstStart event, node.started, node.created
```

Delete blocks the old instance before the new join, so a late old digest is refused by name. The workflow waits on stream events and the node record, not gossip.

## Sequence diagram

```mermaid
sequenceDiagram
    participant M as Mesh-primary
    participant N as Member
    participant P as Provider
    participant F as Fabric-primary
    Note over M: tick: member PendingReconnect
    M->>P: node.runtime.inspect
    P-->>M: exited or os-stopped is the verdict; else running or unavailable
    M->>N: tickle (direct, then via peers), unless the inspect gave the verdict
    Note over M: writes rdm.node.investigated
    Note over M: reached: node.mesh.join; otherwise the recovery, the mesh-primary's own
    Note over M: node.delete → node.deleted (TerminateRuntime first)
    Note over M: node.create, same path and name (the predecessor's data dir when exited and persistent)
    Note over M: each step recorded on the node record; node.started, node.created
```

## States written

Its mesh-primary writes the birth `PendingReconnect` as a self-heal starts and `dead` when the investigation concludes in failure (Q393); its verdict is [node investigation](node-investigation.md)'s `rdm.node.investigated`, and each step writes what [node.delete](node-delete.md) and [node.create](node-create.md) write.

| written | by | when |
|---|---|---|
| `rdm.node.investigated` | the mesh-primary | the verdict is decided |

As stored:

`exited`:

```json
{"len":478,"specversion":"1.0","id":"01a1ae8d-f449-75a0-8b01-f4d18d8ccc70","source":"rdm://whpdxrcdyfvh/fdceh6x7d3y6/d3fbkvnm7w63","type":"rdm.node.investigated","subject":"5edf9fd8kmzc","time":"2026-11-06T01:51:41.126Z","datacontenttype":"application/json","dataschema":"event/1","traceparent":"00-07b2cd84abccf6fa5a1c1b873da2e267-3e5747e696e2eea8-01","data":{"incarnation":"vzrw2nsrfnv9","name":"1","next":"recreate","path":"mesh2.node","verdict":"exited","reason":"self-heal","trigger":"silence"},"crc32":"38f4e36f"}
```

`reason` is `self-heal` or `ordered`; `trigger` is `silence` for a self-heal, else what started the ordered investigation: `connect-refused`, `connect-timed-out`, `connect-failed`, `endpoint-unresolved`, `attempts-spent` or `report-call-failed` (Q393).

## Failure

Failures and stream certainty follow the
[caller execution contract](../node-rpc-envelope.md#caller-execution-and-certainty). A member unheard
during a workflow belongs to that workflow: its Build or node command ends failed, and the
mesh-primary's next tick that finds the member still lost investigates it again and runs the recovery.

One failed recovery is never run again: an adoption launch abandoned because the lost birth or a live
node-admin answered before its first mutation ([fabric recovery](fabric-recovery.md#nothing-alive)),
recorded failed with NameHeld's code; the birth it named, or the admin that answered, is alive. Only
that abandonment is exempt: a recovery that fails on any other name conflict is run again (Q159).

### The data dir

RDM does not look at what is in the predecessor's data dir. Whether its data is usable is the new node's `pre_node_ready` answer: `Ok`, `Pending` while it waits (for example a volume not yet mounted), `Failed` when it is not, which ends the recovery's step failed with the node `Pending` (Q350, Q359).

## Observability contract

Trace propagation, step spans and correlation follow the
[runtime spans](../node-rpc-envelope.md#runtime-spans). The investigation's steps, its verdict and the
submit each have a span on the mesh-primary.
