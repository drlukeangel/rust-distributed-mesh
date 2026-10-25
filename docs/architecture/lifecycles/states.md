# States

[Lifecycle index](../fabric-mesh-node-lifecycles.md) · [Op contracts](../node-rpc-envelope.md#the-op-ledger)

This page owns the fabric, mesh and node lifecycle states, who sets each, setting a state and
its checklist, the one routing rule, stopped nodes, and resize and executable replacement. Each workflow's sequence
lives on its own page; op fields and enums live only in the
[envelope](../node-rpc-envelope.md#status-op-0x1b-rdm); how a state travels to peers is
[gossip](../gossip.md#31-frame-catalog). Application state-sync is an external integration contract.

- **Fabric:** `pending` → `state-sync` → `state-commit` → `ready-for-traffic`. FabricStatus also carries a `degraded` health marker, set by the fabric-primary at a peer mesh's carrier-edge-lost verdict or on the no-member-answers arm's verdict (both arms, R-DG1) and cleared on the new mesh-primary's report; it is not a lifecycle status ([mesh investigation](mesh-investigation.md)). Until the initial fabric first reaches ready-for-traffic, no primary changes and no caller workflow writes a row ([the initial fabric](#fabric-not-ready)).
- **Mesh:** `pending` → `ready-for-traffic` → `deleting` → `deleted`. MeshStatus also carries a `degraded` health marker, set when a mesh is given a new node-admin by the fabric-primary (on either investigation arm, R-DG1) and cleared on the new mesh-primary's report; it is not a lifecycle status. A mesh is alive while it holds a node; only its mesh-primary leaves it, always last, and the mesh is then dead and never restarted. A mesh id is never reissued; deleted is final. A deleted mesh releases its name within the fabric as a deleted node releases its name within its path; a mesh created again under that name is a new mesh with a new id.
- **Node:** starting → pending → ready-for-traffic → connections-deleting → connections-deleted → stopping → stopped → deleting → deleted under the [node object contract](../node-rpc-envelope.md#typed-object-contract). Stopped is a status: a live, stopped process. Start rejoins that same process and restores readiness only after all start gates pass. Delete proves exact runtime exit, writes the durable row deleted and releases identity/name; it is not a return to readiness.
- **Node row silence marks:** a node row may carry the observer-inferred marks `PendingReconnect` and `Dead` (TRUE OFFLINE), written by the node's mesh-primary. They are never themselves a reason to act ([node recovery](node-recovery.md)).

A mesh is ready-for-traffic when every planned exact member birth for the accepted
Build is ready. The fabric enters state-sync when every planned mesh is
ready-for-traffic. The fabric-primary declares fabric state through the completed check-ins.
Neither a Build nor the rectifier ever declares the fabric ready-for-traffic.

## Who sets each state

| state | set by | from |
|---|---|---|
| node starting | the node's mesh-primary | the accepted node.start execution (inside node.create, or on a stopped node) |
| node pending | the node's mesh-primary | the node's accepted join report, on the start call or a commanded join, once per join ([node-mesh-join.md](node-mesh-join.md) step 10) |
| node ReadyForTraffic | the node's mesh-primary | the node's node.started report, after every start gate |
| node connections-deleting | the node's mesh-primary | the node's `ConnectionsDeleting` report on the command stream of `node.connections.delete { scope: All }`, its block on new application work in force; inside node.stop the visible status is stopping |
| node connections-deleted | the node's mesh-primary | the node's node.connections.deleted completion carrying `Established`; a `Deadline` completion leaves the node connections-deleting |
| node stopping | the node's mesh-primary | the node's `CommandAccepted` admission frame on the `StopNode` command stream (0x22); no state before that frame |
| node stopped | the node's mesh-primary | the matching stopped frame on the stop call's stream |
| node deleting | the node's mesh-primary (the fabric-primary for a mesh with no primary) | node.delete's adopted claim, published as NodeDeleting |
| node deleted (the durable row) | the node's mesh-primary (the fabric-primary for a mesh with no primary) | node.delete's step receipt |
| mesh Pending | the node-admin that launched the mesh's first admin | one ApplyMeshState(Pending) sent after the launched birth answers Ping, waiting at most 20 s for that reachability (RDM's `apply_mesh_state` in `rdm-admin`) |
| mesh ReadyForTraffic | the fabric-primary decides and answers `Applied`; the mesh-primary records it on its mesh's record, and the state is durable at that write; the declaration stays owed until then and is answered `Applied` again if repeated (Q144) | its mesh-primary's report that every planned birth is ready |
| mesh deleting | the mesh-primary | the fabric-primary's DeleteMesh admission (mesh.deleting) |
| mesh deleted | the mesh-primary, still alive, on the mesh row (the fabric-primary as acting primary only when no primary survived) | mesh.deleted, from its members' node.delete receipts; never an op |
| fabric state-sync, state-commit, ready-for-traffic | the fabric-primary | the completed check-ins below it |

The internal report and apply that move a node's status are [node status](node-status.md).
Which frame carries each state to peers, and who publishes it, is the
[gossip frame catalog](../gossip.md#31-frame-catalog).

## Setting a state

Fabric status changes are carried down and answered by member check-ins. A
successful admission reply does not set a state: the matching upward completions
and planned-birth checklist do. Required writes finish before their completion check-ins.
Gossip disseminates the resulting states and overlays for peer topology and routing; it never
sets a state, and neither caller progress nor setting a state waits for peers to receive
gossip. Before state-commit, RDM holds the Fabric and Mesh records in the scratchpad; state-commit publishes them to the configured stores. A Node record is written straight to the store, before state-commit too, because a declared state acknowledged as Applied is on it and is durable before the reply; a successor acquires that fact without creating a new lifecycle transition.

| state | carried down | checked in | between |
|---|---|---|---|
| state-sync | the state-sync hook | the hook's answer | external: RDM requests application work; Rafka reports completion to the fabric-primary |
| state-commit | state-commit status | member check-in | internal: fabric-primary → mesh primaries → nodes |
| ready-for-traffic | ready-for-traffic status | member check-in | internal; one notice to Rafka |
| deleting (mesh) | mesh.delete | mesh.deleting | [mesh.delete](mesh-delete.md) |
| deleted (mesh) | none | mesh.deleted | [mesh.delete](mesh-delete.md) |

Each workflow's directed calls, stream events and gossip are on its page in the
[lifecycle index](../fabric-mesh-node-lifecycles.md); Build-backed work follows the
[Build carrier](../node-rpc-envelope.md#build-op-0x20-rdm).

<a id="fabric-not-ready"></a>
### The initial fabric before ready-for-traffic

Until the initial fabric first reaches ready-for-traffic it is pending, in state-sync or in
state-commit, and nothing that changes a primary or writes caller state runs. These are refused by
name, `fabric-not-ready`:

- a Build or update that would change the fabric-primary or a mesh-primary
  ([node.primary.update and node.fabric-primary.update](node-primary-update.md));
- [mesh.delete](mesh-delete.md);
- every node workflow a caller submits that writes a row: node.stop, node.start, node.restart,
  node.delete, node.update and the [replace recipe](node-replace.md).

The initial accepted Build, which creates the fabric's nodes and brings the fabric to
ready-for-traffic, runs as usual. fabric.delete is admitted before the first ready-for-traffic, the Build in progress or not (build.md §1): it ends the fabric, and its own mesh.delete and node.delete steps, and only those, run exempt from this refusal; launch and delete evidence is retained until the teardown completes. Once the fabric is ready-for-traffic these are admitted as
usual. A Build is refused at build.create with the Build family's existing `Rejected` refusal
([Build refusals](../node-rpc-envelope.md#fabric-not-ready-refusal)).
The reason is that before state-commit the Fabric record and each mesh's Mesh record live only in
their writers' scratchpads, so a primary change or a record written in that window is lost with its
writer or cannot be read by its successor.

## Routing

<a id="routing-stop"></a>
Application routing selects a birth only while it is ready-for-traffic and its mesh is
ready-for-traffic. There is no list of excluded states: anything else is a routing
stop, applied by a peer on receipt of the fact that says so. A node that is starting, pending,
connections-deleting, connections-deleted, stopping, stopped or deleting, a NodeStopped,
NodeRestarting, NodeDeleting or NodeDeleted overlay, a mesh deleting or deleted,
each excludes the exact birth from new application traffic from the moment the peer holds it.
In-flight work finishes under [node.connections.delete's](node-connections-delete.md) rules. A routing stop is never a barrier: it
orders nothing and waits for nobody. Start clears NodeStopped at node.joined, but
selection resumes only when the start gates complete, the node and mesh are ready,
and no active routing-stop overlay remains. A late ready digest or stopped overlay
cannot restore a deleted identity or a member of a deleted mesh.
Peers may observe a mesh's final primary go directly from running to Gone when its
NodeDeleted arrives before its NodeStopped; observing Stopped is not a prerequisite to
accepting terminal proof.

Application eligibility does not prevent lifecycle/control calls to a retained
endpoint: a stopped process remains reachable for start, update and delete. Only whoever
selects application targets reads lifecycle; the [resolver](../node-rpc.md#42-live-resolver-contract)
reads none, so a connections-deleting, connections-deleted, stopping or stopped birth still resolves Found and the lifecycle op reaches it.
Deletion separately blocks the deleted identity in the peer-location view, where
the resolver answers Gone. Silence and lack of an application route are not that proof.

What every node observes while a node.delete removes `mesh2.rpc.2`: the accepted topology
changes first, the observed topology only as the executor acts.

| moment | accepted topology | observed: mesh2.rpc.2 | resolver `ExactNode` | application routing |
|---|---|---|---|---|
| Build accepted | does not name it | current | Found | selects it |
| executor claimed, `NodeDeleting` | does not name it | current, overlay `NodeDeleting` | Found | never selects it |
| node.stopped; the mesh-primary's NodeStopped | does not name it | current, status `stopped`, overlay | Found | never selects it |
| provider proof, `NodeDeleted` | does not name it | removed; deleted identity retained | Gone | not a candidate |
| after the deleted retention | does not name it | forgotten | Unknown | not a candidate |

## Stopped nodes

A node at rest is stopped or deleted. Stopped: in the system with its identity kept, receiving no
traffic, silent on gossip and not advancing. Deleted: not in the system.

connections-deleting is live and not routable; node.connections.deleted carries
`ConnectionsReceipt`: Established { in_flight, closed } or, at the bound, Deadline { last_in_flight, closed, held }.
stopped is not live and is still present. node.stop runs `node.connections.delete { scope: All }`,
then cuts its gossip neighbours and its Node RPC pool, which are not connections, but preserves
the node's endpoint and the stop stream, returns node.stopped,
and leaves the live process stopped. node.get answers Current { parked }.

node.start is the only transition out of stopped.
It retains node.id, incarnation, endpoint key, bound port and bin and completes all
start gates before ready-for-traffic. node.restart composes that stop/start; it is not
an exit and a new birth. node.delete proceeds from stopped, as deleting, to `TerminateRuntime`
and identity release, deleted. Silence decides nothing; node investigation does.

The [stopped-node collector](../node-identity-and-state.md#stopped-node-garbage-collector)
submits ordinary node.delete Builds after the configured stopped-node TTL, skipping
births referenced by open Builds. It performs no deletion itself. Blank TTL disables
the sweep; retained storage is not removed by collection.

## Leadership

Node status alone does not change a primary. Ordinary `node.connections.delete { scope: All }`
or node.stop of a primary requires [node.primary.update](node-primary-update.md) first
(node.fabric-primary.update for the fabric-primary);
on the initial fabric no primary changes before ready-for-traffic ([fabric-not-ready](#fabric-not-ready)).
What fabric.delete does with the primaries is [fabric.delete](fabric-delete.md) and [Elections §6](../fabric-node-lifecycle-elections.md#6-fabricdelete-moves-no-primary). Traffic eligibility, the election and the runtime's exit
are separate facts.

## The check-in checklist

- Each primary keeps a checklist of its members. It names the accepted Build's PLANNED
  births: a member counts only when it checks in as its planned birth.
- A primary cannot report the state's upward completion until its own required work
  and every planned member's matching check-in are complete. Individual lifecycle
  status changes do not wait for the entire check-in checklist.
- A new primary, through election or recovery, re-sends the current state's command.
  Members already done check in again, and the checklist rebuilds itself.
- A member leaves the checklist only through the accepted removal or its re-creation,
  never by a timer.
- A check-in is a message the member owes its primary ([Node RPC §43](../node-rpc.md#43-owed-messages-delivery-is-the-runtimes),
  under `Budget::Owed`): delivered until the primary answers it, ended only by that answer, the
  primary's re-creation or accepted removal, or the member dropping it; re-owed to the new primary
  when the primary changes; never ended by a timer. The primary counts a check-in once per
  (state, birth) and answers a repeat `AlreadyApplied`.
- A primary counts only check-ins that arrive after it receives this state's command.
- State-commit is a FABRIC state. A node's or mesh's state-committed reply is a check-in,
  not a node or mesh state.

## Resize and executable replacement

A resize or an executable replacement puts the fabric back to pending; meshes already serving stay ready-for-traffic. Its Build is Complete only when the fabric is ready-for-traffic again from this Build's own change, as fabric.create's is: while the fabric changes, the Build reports Blocked naming the change, never Complete; while the fabric awaits the application's answer the Build reports Blocked naming the blocker, never Complete; a state's check-in or a hook that reports failure fails the Build on that step. During an executable replacement, a member still on its old executable and identity does not satisfy its planned replacement. A new executable requires a replacement node with a new NodeId and incarnation ([build-deployment.md](../build-deployment.md)); node.restart retains the same process and executable. The mesh cannot report ready-for-traffic, and the fabric cannot change, until the replacement's last planned birth checks in. Rafka then answers the state-sync hook at once when no application content work is needed;
the internal state-commit and ready-for-traffic check-ins still run.

See [Fabric state-sync](fabric-state-sync.md), [Fabric state-commit](fabric-state-commit.md), [Fabric ready-for-traffic](fabric-ready-for-traffic.md) and [mesh.delete](mesh-delete.md).
