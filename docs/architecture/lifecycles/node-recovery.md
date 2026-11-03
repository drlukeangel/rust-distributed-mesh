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
| Detection | The mesh-primary's tick (every gossip interval) reads the members its view marks `PendingReconnect`: unheard past the staleness floor, or marked at once when a call to them was never sent for a reachability reason or ended with its attempts spent (step 1) |
| Investigation | [node investigation](node-investigation.md): the by-destination connections index, node.runtime.inspect, node.tickle direct and via peers; one verdict |
| Trigger | The verdict (Q111). Reached: commanded back through node.mesh.join. Any other verdict: re-created. Nothing waits on the provider's `Exited` |
| Execution | The mesh-primary runs the recovery itself and records each step on the node record; the investigation's verdict is its `rdm.node.investigated` event ([build.md §5](../build.md#5-buildstateadapter)). It is never a Build: no build.create, no Build record, nothing claimed. Work on one node is serialized at its mesh-primary: a recovery and any other step on that node (a node command, a Build's step) run one after the other, and a Build in flight elsewhere in the mesh neither holds it nor is held by it. A lost mesh-primary is investigated and recovered by the successor its election names, as any member of its mesh, its own workflow; the fabric-primary investigates and recovers it only when no successor takes the role (Q335) |
| Steps | node.delete of the unheard instance, node.create at the same path and name, keyed on the target birth `(node.id, incarnation)` |
| Retained | path and name, placement and storage (from the node record), config and tags (inherited from the deleted row) and the executable (seeded from the deleted row: a recovered node runs the bin its row records, never the recovering admin's) ; a row written before the executable field decodes with none, and node recovery of that birth is refused by name: the topology's entry is the target, not what the birth ran; the repair is a request naming that path's executable, the ordinary replacement ([build-deployment.md](../build-deployment.md)); the inherited data dir and endpoint key when the verdict is `exited` |
| New | node.id, incarnation, OS-assigned port, exact runtime; a newly allocated data dir and endpoint key on any verdict but `exited`, and when the inherited data dir is absent (`R-FM2`) |
| App part | The new node's start runs [`pre_node_ready`](../fabric-node-hooks.md#pre_node_ready) with start_kind FirstStart and `inherited = true` over a kept data dir, or `inherited = false` over a newly allocated one, then its [`on_node_ready`](../fabric-node-hooks.md#on_node_ready) notice after node.started |
| Lineage | No supersedes: recovery creates a new node.id ([node identity](../node-identity-and-state.md#2-a-node-is-an-entity-and-it-has-three-ids)) |

## Detection and the verdict

The mesh-primary runs one loop for its own members on every gossip interval, never on a timer of its
own. After a recovery step fails by a retryable cause, that exact birth becomes eligible again after a
backoff doubling from the gossip interval to the staleness floor.

1. **Silence.** Each node's pruner marks a member `PendingReconnect` once it is unheard past the
   staleness floor. Nothing is sent; the mark comes from the digests not arriving. Peers stop routing
   new work to it. Silence is not death. Every node-admin keeps the silence bookkeeping
   (`first_seen`), so a node-admin that becomes mesh-primary mid-window continues the window already
   running. The mesh-primary also marks a member `PendingReconnect` at once when a command to it was
   never sent for a reachability reason (the connect refused, timed out or failed; the endpoint
   unresolved), and when a call to it ends with its attempts spent: its own call, or one another
   caller reports with Status `ReportCallFailed` ([Node RPC §43](../node-rpc.md#43-resend-delivery-is-the-runtimes)).
   The mark is an observer mark: it never replaces a lifecycle state the node reported and
   the mesh-primary recorded (`connections-deleting`, `stopping`, `stopped`), a silence mark ends when a digest
   of the exact birth arrives after it or when the birth answers its tickle; a spent-call mark ends only
   when the exact birth answers, a call of this primary's or the investigation's tickle; either ends
   when the view no longer holds that birth. A birth heard again runs its `pre_node_ready` hook again before it is
   ready ([fabric-node-hooks.md](../fabric-node-hooks.md#where-it-runs)).
2. **Investigation.** Only the mesh-primary acts. Half a floor after the mark it runs
   [node investigation](node-investigation.md) on the exact birth: the by-destination connections index, node.runtime.inspect (`exited` or `os-stopped` is the verdict, and no tickle is sent), node.tickle direct, the same tickle via up to two eligible live peers of the birth's mesh sent at once. Any answer from the exact birth ends the ladder and cancels the
   probes still running; one failed peer probe does not cancel the other; every probe names the same
   exact `(node.id, incarnation)`. For a lost mesh-primary the successor its election names runs it as for
   any member ([elections §4.1](../fabric-node-lifecycle-elections.md#41-the-nodes-of-one-type-in-one-mesh));
   the fabric-primary runs it, without the half-floor wait, only when the mesh's aggregate stays unheard past the staleness floor: no successor took the role (Q335).
3. **The verdict is the one decision (Q111).** `reached`: a held birth that answered, or that is heard
   again by its digest, is commanded back through [node.mesh.join](node-mesh-join.md) by the primary
   above it (its mesh-primary; the fabric-primary for a mesh-primary: every admin looks, one commands)
   and runs its start sequence again before it is ready (Q87). A lost node-admin that held a primary role takes none back: commanded back, it unmarks itself as primary before it is ready and takes the mesh's primary from the successor's record; re-created, its new birth starts with no primary role. The command goes only to a
   birth the view holds off ready (the topology decides it, not what an admin just started
   has heard), once per silence (not again within a staleness floor, nor until it is heard
   ready or unheard again), never to the fabric-primary, which has no primary above it, and
   never to a birth with a command of its primary open or in flight, which is that command's. A
   commanded start that stops at a step leaves the birth off its channels, Pending, for the next
   command. Any other verdict (`running`, `os-stopped`, `exited`, `unproven`): the recovery below, at
   once. Nothing holds, nothing is inspected again, and nothing waits for the provider's `Exited`
   (Q110, Q111). A birth that comes back once its verdict is not `reached`, during its replacement's birth or after it, is a deleted birth: every view refuses
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
   node record; the next tick after its backoff decides again.
3. [node.create](node-create.md) at the same path and name, its spec naming the old data dir when the
   verdict was `exited` (or a newly allocated data dir otherwise, and when the old data dir is absent,
   `R-FM2`): it loads the endpoint key from the kept data dir (or mints a new endpoint key in the fresh
   data dir) and binds port 0, with a new node.id, incarnation and port. Its config is seeded from the
   deleted row at that path and name.
4. Create runs the whole [start](node-start.md#end-to-end-sequence) with FirstStart and
   `inherited = true` over the kept data dir (or `inherited = false` over a newly allocated one), and
   ends node.created.

## Command, completion and gossip messages

```text
mesh-primary tick: PendingReconnect → node investigation → verdict
  reached:    node.mesh.join (commanded back)
  otherwise:  the mesh-primary runs the recovery
                node.delete(unheard instance) → node.deleted   (TerminateRuntime first; no stop frames)
                node.create(same path and name; inherited data dir when exited) → every FirstStart event, node.started, node.created
```

Delete blocks the old instance before the new join, so a late old digest cannot return the retained
endpoint key to the old port. The workflow waits on stream events and the node record, not gossip.

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
    Note over M: reached: node.mesh.join; otherwise the recovery, the mesh-primary's own
    Note over M: node.delete → node.deleted (TerminateRuntime first)
    Note over M: node.create, same path and name (inherited data dir when exited)
    Note over M: each step recorded on the node record; node.started, node.created
```

## Failure

Failures and stream certainty follow the
[caller execution contract](../node-rpc-envelope.md#caller-execution-and-certainty). A member unheard
during a workflow belongs to that workflow: its Build or node command ends failed, and the
mesh-primary's next tick after its backoff investigates the member again and runs the recovery.

One failed recovery is never run again: an adoption launch abandoned because the lost birth or a live
node-admin answered before its first mutation ([fabric recovery](fabric-recovery.md#nothing-alive)),
recorded failed with NameHeld's code; the birth it named, or the admin that answered, is alive. Only
that abandonment is exempt: a recovery that fails on any other name conflict is run again (Q159).

### Lost data dir (`R-FM2`)

Gone versus broken is decided from the filesystem error on the inherited `data_dir`, never guessed. An
absent directory path (`NotFound` on the directory itself) is **gone**; an existing directory on disk
whose `node-key` is missing, unreadable, or corrupt (`NotFound` on `node-key` inside an existing
directory, `EIO`, `EACCES`, or decode failure) is **broken**:

- **Gone (arm A).** When the `data_dir` of a birth whose verdict was `exited` is absent across
  `RDM_DATA_DIR_GONE_FLOORS` staleness floors (default 2 = 60 s at the 30 s floor, to ride out a
  remount), the mesh-primary records `data-dir-lost { path, since }` durably on that birth
  (`rdm.node_admin.node.update.via-data-dir-lost`, surfaced on `node.get` and never as a gossip
  `MemberStatus`), and the recovery creates the node at the same `path` and `name` over a newly
  allocated `data_dir` beside the old path with a newly minted endpoint key (the old key is never
  re-minted) and `pre_node_ready` with `inherited = false`. On any verdict but `exited` the data dir is
  newly allocated without the wait ([node investigation](node-investigation.md)); for a lost mesh's
  node-admin, the fabric-primary runs this arm ([elections §4.3](../fabric-node-lifecycle-elections.md#43-fabric-primary)).
- **Broken (arm B).** On the first `PrepareStorage` failure over an existing but unreadable `data_dir`,
  the executor records `data-dir-unreadable { path, cause }` durably on that birth
  (`rdm.node_admin.node.reject.via-data-dir-unreadable`, surfaced on `node.get` and never as a gossip
  `MemberStatus`), and the mesh-primary's due rule holds that recovery (not due while the fact stands)
  instead of resubmitting every tick. RDM deletes nothing on disk. When `<data_dir>/node-key` is
  repaired and reads again, the next tick clears `data-dir-unreadable`
  (`rdm.node_admin.node.resolve.via-data-dir-repaired`) and completes recovery over the kept `data_dir`
  with its retained endpoint key (`inherited = true`).

## Observability contract

Trace propagation, step spans and correlation follow the
[runtime spans](../node-rpc-envelope.md#runtime-spans). The investigation's steps, its verdict and the
submit each have a span on the mesh-primary.
