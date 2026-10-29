# Node recovery

[Lifecycle index](../fabric-mesh-node-lifecycles.md) · [States](states.md) · [Typed calls and wire mapping](../node-rpc-envelope.md#typed-object-contract) · [node investigation](node-investigation.md)

Node recovery is RDM's drift reconciliation for a member that goes unheard outside any workflow. The
accepted Build is the fabric's desired topology; a member at a path.name that
[node investigation](node-investigation.md) did not reach is drift, and RDM puts it back. The
mesh-primary is the reconciler for its own members, whatever their type: it tracks them, runs the
investigation and starts the recovery. The app takes part only through the start hook of the new node.
Recovery is not a caller op. (A mesh none of whose node-admins can be reached is
[mesh investigation](mesh-investigation.md), driven by the fabric-primary; its decision step runs
node.create of a node-admin into the live mesh.)

## Operation summary

| Field | Contract |
|---|---|
| Reconciler | The mesh-primary of the member's mesh; the fabric-primary for a mesh-primary (mesh investigation for a mesh with no reachable node-admin) |
| Detection | The mesh-primary's tick (every gossip interval) reads the members its view marks `PendingReconnect`: unheard past the staleness floor, or marked at once when a command to them was never sent for a reachability reason (step 1) |
| Investigation | [node investigation](node-investigation.md): the by-destination connections index, node.runtime.inspect, node.tickle direct and via peers; one verdict |
| Trigger | The verdict (Q111). Reached: commanded back through node.mesh.join. Any other verdict: re-created. Nothing waits on a proof of termination |
| Execution | The mesh-primary opens the recovery as the next attempt of the current Build at `(build_id, mesh, attempt)` on its mesh's rows, claims it there and runs it through its own executor, writing each receipt and the attempt's verdict to the Build journal on the store root, where the fabric-primary reads them (R-BI7); it makes no call to the fabric-primary, which may be offline. An unheard mesh-primary's recovery is the fabric-primary's ([mesh investigation](mesh-investigation.md)) |
| Steps | node.delete of the unheard instance, node.create at the same path.name; the Build's steps are keyed `node.delete:<path.name>` and `node.create:<path.name>` |
| Retained | path.name, placement and storage (from the path's create spec, which a Build has applied to the row), config and tags (inherited from the deleted row) and the executable (seeded from the deleted row: a recovered node runs the bin its row records, never the recovering admin's) ; a row written before the executable field decodes with none, and node recovery of that birth is refused by name: the topology's entry is the target, not what the birth ran; the repair is a request naming that path's executable, the ordinary replacement ([build-deployment.md](../build-deployment.md)); the inherited data dir and endpoint key when the verdict is `exited` |
| New | node.id, incarnation, OS-assigned port, exact runtime; a newly allocated data dir and endpoint key on any verdict but `exited`, and when the inherited data dir is absent (`R-FM2`) |
| App part | The new node's start runs [`pre_node_ready`](../fabric-node-hooks.md#pre_node_ready) with start_kind FirstStart and `inherited = true` over a kept data dir, or `inherited = false` over a newly allocated one, then its [`on_node_ready`](../fabric-node-hooks.md#on_node_ready) notice after node.started |
| Lineage | No supersedes: recovery creates a new node.id ([node identity](../node-identity-and-state.md#2-a-node-is-an-entity-and-it-has-three-ids)) |

## Detection and the verdict

The mesh-primary runs one loop for its own members on every gossip interval, never on a timer of its
own. After a retryable recovery-submit failure, that exact birth becomes eligible again after a backoff
doubling from the gossip interval to the staleness floor.

1. **Silence.** Each node's pruner marks a member `PendingReconnect` once it is unheard past the
   staleness floor. Nothing is sent; the mark comes from the digests not arriving. Peers stop routing
   new work to it. Silence is not death. Every node-admin keeps the silence bookkeeping
   (`first_seen`), so a node-admin that becomes mesh-primary mid-window continues the window already
   running. The mesh-primary also marks a member `PendingReconnect` at once when a command to it was
   never sent for a reachability reason (the connect refused, timed out or failed; the endpoint
   unresolved). The mark is an observer mark: it never replaces a lifecycle state the node reported and
   the mesh-primary recorded (`connections-deleting`, `stopping`, `stopped`), and it ends when a digest
   of the exact birth arrives after it, when the birth answers its tickle, or when the view no longer
   holds that birth. A birth heard again runs its `pre_node_ready` hook again before it is
   ready ([fabric-node-hooks.md](../fabric-node-hooks.md#where-it-runs)).
2. **Investigation.** Only the mesh-primary acts. Half a floor after the mark it runs
   [node investigation](node-investigation.md) on the exact birth: the by-destination connections index, node.runtime.inspect (`exited` or `os-stopped` is the verdict, and no tickle is sent), node.tickle direct, the same tickle via up to two eligible live peers of the birth's mesh sent at once. Any answer from the exact birth ends the ladder and cancels the
   probes still running; one failed peer probe does not cancel the other; every probe names the same
   exact `(node.id, incarnation)`. For an unheard mesh-primary the fabric-primary runs it, without the
   half-floor wait, after the election has named the new mesh-primary
   ([elections §4.1](../fabric-node-lifecycle-elections.md#41-the-nodes-of-one-type-in-one-mesh)); outside its
   mesh the ladder is [mesh investigation](mesh-investigation.md)'s.
3. **The verdict is the one decision (Q111).** `reached`: a held birth that answered, or that is heard
   again by its digest, is commanded back through [node.mesh.join](node-mesh-join.md) by the primary
   above it (its mesh-primary; the fabric-primary for a mesh-primary: every admin looks, one commands)
   and runs its start sequence again before it is ready (Q87). The command goes only to a
   birth the view holds off ready (the topology decides it, not what an admin just started
   has heard), once per silence (not again within a staleness floor, nor until it is heard
   ready or unheard again), never to the fabric-primary, which has no primary above it, and
   never to a birth with a command of its primary open or in flight, which is that command's. A
   commanded start that stops at a step leaves the birth off its channels, Pending, for the next
   command. Any other verdict (`running`, `os-stopped`, `exited`, `unproven`): the recovery below, at
   once. Nothing holds, nothing is inspected again, and nothing waits for a proof of termination
   (Q110, Q111). A birth that comes back after it was re-created is a deleted birth: every view refuses
   its Digest by name ([gossip.md receiver rules](../gossip.md#31-frame-catalog)), and its mesh-primary
   decides it by its path.name and its atlas ([node investigation](node-investigation.md), Q207): its
   re-created birth is there, so it is reported to the fabric-primary, which terminates its runtime.

## Numbered end-to-end sequence

1. The mesh-primary's investigation ends in a verdict other than `reached`, and it opens the recovery
   as the next attempt of the current Build at `(build_id, mesh, attempt)` on its mesh's rows, keyed on
   the target birth `(node.id, incarnation)` (for an unheard mesh-primary, the fabric-primary opens it
   itself). A repeat answers AlreadyApplied or BuildInProgress, never a second recovery.
2. The mesh-primary claims the attempt on its mesh's rows and runs it through its own executor, writing
   each receipt and the verdict to the Build journal on the store root, where the fabric-primary reads
   them (R-BI7); no call to the fabric-primary is made.
3. [node.delete](node-delete.md#numbered-end-to-end-sequence) of the target instance, as a target with
   no RPC to the old process and no stop frames. Its first step is
   `TerminateRuntime { node_id, incarnation, handle }` on the exact runtime handle recorded at launch,
   read at the step's instant and never skipped for what it reads, except `os-stopped`, which recovery
   leaves as it is ([node.delete](node-delete.md)). A `running` runtime is the provider's
   `terminate(handle, Graceful)` then `Forced` after the stop grace: Q110's "then you kill it". An
   `exited` runtime is the same call: the provider removes what the runtime left (a container and the
   address it held, its logs kept beside the data dir first; a process leaves nothing), so no runtime of
   a superseded birth stays on the host; a runtime already removed terminates as done (Q145). A
   terminate the provider refuses ends the attempt `Failed { step: TerminateRuntime, reason }` by name;
   the next tick after its backoff decides again.
4. [node.create](node-create.md) at the same path.name, its spec naming the old data dir when the
   verdict was `exited` (or a newly allocated data dir otherwise, and when the old data dir is absent,
   `R-FM2`): it loads the endpoint key from the kept data dir (or mints a new endpoint key in the fresh
   data dir) and binds port 0, with a new node.id, incarnation and port. Its config is seeded from the
   deleted row at that path.name.
5. Create runs the whole [start](node-start.md#end-to-end-sequence) with FirstStart and
   `inherited = true` over the kept data dir (or `inherited = false` over a newly allocated one), and
   ends node.created.

## Command, completion and gossip messages

```text
mesh-primary tick: PendingReconnect → node investigation → verdict
  reached:    node.mesh.join (commanded back)
  otherwise:  open attempt N of the current Build, run
                node.delete(unheard instance) → node.deleted   (TerminateRuntime first; no stop frames)
                node.create(same path.name; inherited data dir when exited) → every FirstStart event, node.started, node.created
```

Delete blocks the old instance before the new join, so a late old digest cannot return the retained
endpoint key to the old port. The workflow waits on stream events and Build receipts, not gossip.

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
    Note over M: reached: node.mesh.join; otherwise open attempt N of the current Build, run
    Note over M: node.delete → node.deleted (TerminateRuntime first)
    Note over M: node.create, same path.name (inherited data dir when exited)
    M-->>F: receipts up the attempt's stream; node.started, node.created
```

## Failure and receipts

Failures and stream certainty follow the
[caller execution contract](../node-rpc-envelope.md#caller-execution-and-certainty); here the
mesh-primary is the submitter that resubmits by `build_id`. A member unheard during a workflow belongs
to that workflow: its Build ends Failed, and the mesh-primary's next tick after its backoff investigates
the member again and submits the recovery.

One failed attempt owes no exit and is never submitted again: an adoption launch's claimed attempt
abandoned because the lost birth or a live node-admin answered before its first mutation
([fabric recovery](fabric-recovery.md#nothing-alive)), closed `Failed` with NameHeld's code; the birth it
named, or the admin that answered, is alive. Only that abandonment is exempt: a recovery that fails on
any other name conflict owes its exit, and the mesh-primary submits it again (Q159).

### Lost data dir (`R-FM2`)

Gone versus broken is decided from the filesystem error on the inherited `data_dir`, never guessed. An
absent directory path (`NotFound` on the directory itself) is **gone**; an existing directory on disk
whose `node-key` is missing, unreadable, or corrupt (`NotFound` on `node-key` inside an existing
directory, `EIO`, `EACCES`, or decode failure) is **broken**:

- **Gone (arm A).** When the `data_dir` of a birth whose verdict was `exited` is absent across
  `RDM_DATA_DIR_GONE_FLOORS` staleness floors (default 2 = 60 s at the 30 s floor, to ride out a
  remount), the mesh-primary records `data-dir-lost { path, since }` durably on that birth
  (`rdm.node_admin.node.update.via-data-dir-lost`, surfaced on `nodes.get` and never as a gossip
  `MemberStatus`), and the recovery Build creates the node at the same `path.name` over a newly
  allocated `data_dir` beside the old path with a newly minted endpoint key (the old key is never
  re-minted) and `pre_node_ready` with `inherited = false`. On any verdict but `exited` the data dir is
  newly allocated without the wait ([node investigation](node-investigation.md)); for a lost mesh's
  node-admin, the fabric-primary runs this arm ([elections §4.3](../fabric-node-lifecycle-elections.md#43-fabric-primary)).
- **Broken (arm B).** On the first `PrepareStorage` failure over an existing but unreadable `data_dir`,
  the executor records `data-dir-unreadable { path, cause }` durably on that birth
  (`rdm.node_admin.node.reject.via-data-dir-unreadable`, surfaced on `nodes.get` and never as a gossip
  `MemberStatus`), and the mesh-primary's due rule holds that recovery (not due while the fact stands)
  instead of resubmitting every tick. RDM deletes nothing on disk. When `<data_dir>/node-key` is
  repaired and reads again, the next tick clears `data-dir-unreadable`
  (`rdm.node_admin.node.resolve.via-data-dir-repaired`) and completes recovery over the kept `data_dir`
  with its retained endpoint key (`inherited = true`).

## Observability contract

Trace propagation, step spans and correlation follow the
[runtime spans](../node-rpc-envelope.md#runtime-spans). The investigation's steps, its verdict and the
submit each have a span on the mesh-primary.
