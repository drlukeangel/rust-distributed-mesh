# States

[Lifecycle index](../fabric-mesh-node-lifecycles.md) · [Op contracts](../node-rpc-envelope.md#the-op-ledger)

This page owns the fabric, mesh and node lifecycle states, who sets each, the status rounds and
their checklist, the one routing rule, stopped nodes, and resize/roll. Each workflow's sequence
lives on its own page; op fields and enums live only in the
[envelope](../node-rpc-envelope.md#status-op-0x1b-rdm); how a state travels to peers is
[gossip](../gossip.md#31-frame-catalog). Application state-sync is an external integration contract.

- **Fabric:** `pending` → `state-sync` → `state-commit` → `ready-for-traffic`. FabricStatus also carries a `degraded` health marker, set by the fabric-primary at a peer mesh's carrier-edge-lost verdict or on the fully crashed arm upon exact provider exit proof (both arms, R-DG1) and cleared on the new mesh-primary's report; it is not a lifecycle status ([mesh investigation](mesh-investigation.md)). Until the initial fabric first reaches ready-for-traffic, no seat moves and no caller workflow writes a row ([the initial fabric](#fabric-not-ready)).
- **Mesh:** `pending` → `ready-for-traffic` → `deleting` → `deleted`. MeshStatus also carries a `degraded` health marker, set when a mesh is given a new node-admin by the fabric-primary (on either investigation arm, R-DG1) and cleared on the new mesh-primary's report; it is not a lifecycle status. A mesh is alive while it holds a node; only its mesh-primary leaves it, always last, and the mesh is then dead and never restarted. A mesh id is never reissued; deleted is final. A deleted mesh releases its name within the fabric as a deleted node releases its name within its path; a mesh created again under that name is a new mesh with a new id.
- **Node:** starting → pending → ready-for-traffic → connections-deleting → connections-deleted → stopping → stopped → deleting → deleted under the [node object contract](../node-rpc-envelope.md#typed-object-contract). Stopped is a status: a live, stopped process. Start rejoins that same process and restores readiness only after all start gates pass. Delete proves exact runtime exit, writes the durable row deleted and releases identity/name; it is not a return to readiness.
- **Node row silence marks:** a node row may carry the observer-inferred marks `PendingReconnect` and `Dead` (TRUE OFFLINE), written by the node's mesh-primary. They are never themselves a reason to act ([node recovery](node-recovery.md)).

A mesh is ready-for-traffic when every planned exact member birth for the accepted
Build is ready. The fabric enters state-sync when every planned mesh is
ready-for-traffic. The fabric-primary declares fabric state through the completed rounds.
Neither a Build nor the rectifier ever declares the fabric ready-for-traffic.

## Who sets each state

| state | set by | from |
|---|---|---|
| node starting | the node's mesh-primary | the accepted node.start execution (inside node.create, or on a stopped node) |
| node pending | the node's mesh-primary | the node's node.joined report on the start call |
| node ReadyForTraffic | the node's mesh-primary | the node's node.started report, after every start gate |
| node connections-deleting | the node's mesh-primary | the node's `ConnectionsDeleting` report on the command stream of `node.connections.delete { scope: All }`, its block on new application work in force; inside node.stop the visible status is stopping |
| node connections-deleted | the node's mesh-primary | the node's node.connections.deleted completion carrying `Established`; a `Deadline` completion leaves the node connections-deleting |
| node stopping | the node's mesh-primary | the node's `CommandAccepted` admission frame on the `StopNode` command stream (0x22); no state before that frame |
| node stopped | the node's mesh-primary | the matching stopped frame on the stop call's stream |
| node deleting | the node's mesh-primary (the fabric-primary for a mesh with no primary) | node.delete's adopted claim, published as NodeDeleting |
| node deleted (the durable row) | the node's mesh-primary (the fabric-primary for a mesh with no primary) | node.delete's exact provider exit proof |
| mesh Pending | the node-admin that launched the mesh's first admin | one ApplyMeshState(Pending) sent after the launched birth answers Ping, waiting at most 20 s for that reachability (RDM's `apply_mesh_state` in `rafka-node-admin-core`) |
| mesh ReadyForTraffic | the fabric-primary | its mesh-primary's report that every planned birth is ready |
| mesh deleting | the mesh-primary | the fabric-primary's DeleteMesh admission (mesh.deleting) |
| mesh deleted | the mesh-primary, still alive, on the mesh row (the fabric-primary as acting primary only when no primary survived) | mesh.deleted, from its members' exact exit proofs; never an op |
| fabric state-sync, state-commit, ready-for-traffic | the fabric-primary | the completed round below it |

The internal report and apply that move a node's status are [node status](node-status.md).
Which frame carries each state to peers, and who publishes it, is the
[gossip frame catalog](../gossip.md#31-frame-catalog).

## Rounds

Fabric status changes are carried down in rounds and answered by member check-ins. A
successful admission reply does not complete a round: the matching upward completions
and planned-birth checklist do. Required writes finish before their completion check-ins.
Gossip disseminates the resulting states and overlays for peer topology and routing; it never
completes a round, and neither caller progress nor round completion waits for peers to receive
gossip. Before state-commit, RDM holds its entity rows in the scratchpad; state-commit publishes the required held rows to the configured stores. A node declared-state fact acknowledged as Applied is written to `status/<mesh>/declared` on the shared store root before the reply, including before state-commit; a successor acquires that fact without creating a new lifecycle transition.

| state | carried down | checked in | between |
|---|---|---|---|
| state-sync | the state-sync hook | the hook's answer | external: RDM requests application work; Rafka reports completion to the fabric-primary |
| state-commit | state-commit status | member check-in | internal: fabric-primary → mesh primaries → nodes |
| ready-for-traffic | ready-for-traffic status | member check-in | internal rounds; one notice to Rafka |
| deleting (mesh) | mesh.delete | mesh.deleting | [mesh.delete](mesh-delete.md) |
| deleted (mesh) | none | mesh.deleted | [mesh.delete](mesh-delete.md) |

Each workflow's directed calls, stream events and gossip are on its page in the
[lifecycle index](../fabric-mesh-node-lifecycles.md); Build-backed work follows the
[Build carrier](../node-rpc-envelope.md#build-op-0x20-rdm).

<a id="fabric-not-ready"></a>
### The initial fabric before ready-for-traffic

Until the initial fabric first reaches ready-for-traffic it is pending, in state-sync or in
state-commit, and nothing that moves a seat or writes caller state runs. These are refused by
name, `fabric-not-ready`:

- a Build or update that would move a fabric-primary or mesh-primary seat
  ([node.primary.update and node.fabric-primary.update](node-primary-update.md));
- [mesh.delete](mesh-delete.md);
- every node workflow a caller submits that writes a row: node.stop, node.start, node.restart,
  node.delete, node.update and the [replace recipe](node-replace.md).

The initial accepted Build, which creates the fabric's nodes and brings the fabric to
ready-for-traffic, runs as usual. Once the fabric is ready-for-traffic these are admitted as
usual. A Build is refused at build.create with the Build family's existing `Rejected` refusal
([Build refusals](../node-rpc-envelope.md#fabric-not-ready-refusal)).
The reason is that before state-commit the Fabric row, each mesh's Mesh row and its members'
Node rows live only in their holders' scratchpads, so a seat that moved or a row written in that
window is lost with its holder or cannot be read by its successor.

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
Deletion separately blocks the deleted identity in the peer-location book, where
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
an exit/relaunch. node.delete proceeds from stopped, as deleting, to exact provider exit proof
and identity release, deleted. Silence is never exit proof.

The [stopped-node collector](../node-identity-and-state.md#stopped-node-garbage-collector)
submits ordinary node.delete Builds after the configured stopped-node TTL, skipping
births referenced by open Builds. It performs no deletion itself. Blank TTL disables
the sweep; retained storage is not removed by collection.

## Leadership

Node status alone does not surrender a primary seat. Ordinary `node.connections.delete { scope: All }`
or node.stop of a seat holder requires [node.primary.update](node-primary-update.md) first
(node.fabric-primary.update for the fabric seat);
on the initial fabric no seat moves before ready-for-traffic ([fabric-not-ready](#fabric-not-ready)).
What fabric.delete does with seats is [fabric.delete](fabric-delete.md) and [Elections §6](../fabric-node-lifecycle-elections.md#6-fabricdelete-moves-no-seat). Traffic eligibility, committed leadership and exact runtime exit
are separate facts.

## The round checklist

- Each primary keeps a checklist of its members. It names the accepted Build's PLANNED
  births: a member counts only when it checks in as its planned birth.
- A primary cannot report the round's upward completion until its own required work
  and every planned member's matching check-in are complete. Individual lifecycle
  status changes do not wait for the entire round checklist.
- A new primary, through election or recovery, re-sends the current round.
  Members already done check in again, and the checklist rebuilds itself.
- A member leaves the checklist only through the accepted removal and a proven exit,
  never by a timer.
- A check-in is a message the member owes its primary ([Node RPC §43](../node-rpc.md#43-owed-messages-delivery-is-the-runtimes),
  under `Budget::Owed`): delivered until the primary answers it, ended only by that answer, the
  primary's proven exit or accepted removal, or the member dropping it; re-owed to the new holder
  when the primary's seat moves; never ended by a timer. The primary counts a check-in once per
  (round, birth) and answers a repeat `AlreadyApplied`.
- A primary counts only check-ins that arrive after it receives this round.
- State-commit is a FABRIC state. A node's or mesh's state-committed reply is a check-in,
  not a node or mesh state.

## Resize and roll

A resize or roll puts the fabric back to pending; meshes already serving stay ready-for-traffic. During a bin roll, a member still on its
old binary/identity does not satisfy its planned replacement. A new binary requires
a replacement node with a new NodeId and incarnation; node.restart retains the same
process and binary. The mesh cannot report
ready-for-traffic, and the fabric cannot climb, until the roll's last planned birth checks
in. Rafka then answers the state-sync hook at once when no application content work is needed;
the internal state-commit and ready-for-traffic rounds still run.

See [Fabric state-sync](fabric-state-sync.md), [Fabric state-commit](fabric-state-commit.md), [Fabric ready-for-traffic](fabric-ready-for-traffic.md) and [mesh.delete](mesh-delete.md).
