# States

[Lifecycle index](../fabric-mesh-node-lifecycles.md) · [Op contracts](../node-rpc-envelope.md#the-op-ledger)

This page owns the fabric, mesh and node lifecycle states, who sets each, setting a state and
its checklist, the one routing rule, stopped nodes, and resize and executable replacement. Each workflow's sequence
lives on its own page; op fields and enums live only in the
[envelope](../node-rpc-envelope.md#status-op-0x1b-rdm); how a state travels to peers is
[gossip](../gossip.md#31-frame-catalog).

- **Fabric:** `pending` → `ready`. FabricStatus also carries a `degraded` health marker, set by the fabric-primary at a peer mesh's carrier-edge-lost verdict or on the no-member-answers arm's verdict (both arms, R-DG1) and cleared on the new mesh-primary's report; it is not a lifecycle status ([mesh investigation](mesh-investigation.md)).
- **Mesh:** `pending` → `ready` → `deleting` → `deleted`. MeshStatus also carries a `degraded` health marker, set when a mesh is given a new node-admin by the fabric-primary (on either investigation arm, R-DG1) and cleared on the new mesh-primary's report; it is not a lifecycle status. A mesh is alive while it holds a node; only its mesh-primary leaves it, always last, and the mesh is then dead and never restarted. A mesh id is never reissued; deleted is final. A deleted mesh releases its name within the fabric as a deleted node releases its name within its path; a mesh created again under that name is a new mesh with a new id.
- **Node:** starting → pending → ready → connections-deleting → connections-deleted → stopping → stopped → deleting → deleted under the [node object contract](../node-rpc-envelope.md#typed-object-contract). Stopped is a status: a live, stopped process. Start rejoins that same process and restores readiness only after all start gates pass. Delete proves exact runtime exit, writes the durable row deleted and releases identity/name; it is not a return to readiness.
- **Node row silence marks:** a node row may carry the observer-inferred marks `PendingReconnect` and `Dead` (TRUE OFFLINE), written by the node's mesh-primary. They are never themselves a reason to act ([node recovery](node-recovery.md)).

A mesh is ready when every planned exact member birth for the accepted
Build that is not stopped is ready: a stopped birth is at rest by command (below), receives no
traffic and holds neither its mesh nor a checklist (Q240). When every planned mesh is ready the fabric-primary sets the fabric
ready (Q174); no node is commanded to open traffic and none checks in.
Neither a Build nor the rectifier ever declares the fabric ready.

## Who sets each state

| state | set by | from |
|---|---|---|
| node starting | the node's mesh-primary | a stopped node's `CommandAccepted` admission frame on the `StartNode` command stream (0x22); a create's launch, where no process can acknowledge |
| node pending | the node's mesh-primary | the node's accepted join report, on the start call or a commanded join, once per join ([node-mesh-join.md](node-mesh-join.md) step 10) |
| node ready | the node's mesh-primary | the node's node.started report, after every start gate ([`pre_node_ready`](../fabric-node-hooks.md#pre_node_ready) among them); the node then calls [`on_node_ready`](../fabric-node-hooks.md#on_node_ready) |
| node connections-deleting | the node's mesh-primary | the node's `ConnectionsDeleting` report on the command stream of `node.connections.delete { scope: All }`, its block on new application work in force, standalone or as the acknowledgement on node.stop's stream |
| node connections-deleted | the node's mesh-primary | the node's node.connections.deleted completion carrying `Established`, standalone or `StopConnectionsDeleted` on node.stop's stream; a `Deadline` completion leaves the node connections-deleting |
| node leaving | the node's mesh-primary | the acknowledgement of node.mesh.leave: `CommandAccepted` on the `LeaveMesh` command stream (0x22), or `Leaving` on node.stop's stream |
| node left | the node's mesh-primary | node.mesh.leave's completion: `MeshLeft` on the `LeaveMesh` command stream, or `StopMeshLeft` on node.stop's stream |
| node stopping | the node's mesh-primary | the node's `CommandAccepted` admission frame on the `StopNode` command stream (0x22); no state before that frame |
| node stopped | the node's mesh-primary | the matching stopped frame on the stop call's stream |
| node deleting | the node's mesh-primary (the fabric-primary for a mesh with no primary) | node.delete's adopted claim, published as NodeDeleting |
| node deleted (the durable row) | the node's mesh-primary (the fabric-primary for a mesh with no primary) | node.delete's step receipt |
| mesh Pending | the node-admin that launched the mesh's first admin | one ApplyMeshState(Pending) sent after the launched birth answers Ping, waiting at most 20 s for that reachability (RDM's `apply_mesh_state` in `rdm-admin`) |
| mesh ready | the fabric-primary decides and answers `Applied`; the mesh-primary records it on its mesh's record, and the state is durable at that write; the declaration stays owed until then and is answered `Applied` again if repeated (Q144). The write that first puts it on the record calls [`on_mesh_ready`](../fabric-node-hooks.md#on_mesh_ready) | its mesh-primary's report that every planned birth is ready |
| mesh deleting | the mesh-primary | the fabric-primary's DeleteMesh admission (mesh.deleting) |
| mesh deleted | the mesh-primary, still alive, on the mesh row (the fabric-primary as acting primary only when no primary survived) | mesh.deleted, from its members' node.delete receipts; never an op |
| fabric ready | the fabric-primary | every mesh the accepted Build names ready, its attempt claimed; the fabric-primary then calls [`on_fabric_ready`](../fabric-node-hooks.md#on_fabric_ready) |

start and stop are workflows the node runs, and each primitive is one command: the node's acknowledgement of a command sets its "ing" state (starting, stopping, connections-deleting, leaving), its completion sets the "ed" state (ready, stopped, connections-deleted, left), and each is the row's one status, never a second field. delete is the mesh-primary's workflow: no delete command reaches the node; the mesh-primary writes deleting from its adopted claim, composes the node's stop, whose frames move the row as above, terminates the exact runtime and writes deleted from the receipt. A query answers the state the row last took from the node's frame or the mesh-primary's receipt; a command the node has not acknowledged moves the row nowhere. Where no node can report, the mesh-primary writes the state itself: a create's starting from the launch, and an exited node's deleting and deleted from the claim and the receipt.

The internal report and apply that move a node's status are [node status](node-status.md).
Which frame carries each state to peers, and who publishes it, is the
[gossip frame catalog](../gossip.md#31-frame-catalog).

<a id="the-fabrics-ready"></a>
## The fabric's ready

When every mesh the accepted Build names is ready and the Build's attempt is claimed, the
fabric-primary sets the fabric ready, publishes FabricStatus ready under the
[frame catalog](../gossip.md#31-frame-catalog), and the application receives one
[`on_fabric_ready`](../fabric-node-hooks.md#on_fabric_ready) notice, on which Rafka opens client
traffic (Q174, Q175). No node is commanded to open traffic and none checks in. The fabric-primary's
change span (`rdm.node_admin.fabric.update.via-change`) holds, in order,
`rdm.node_admin.fabric.update.via-every-mesh-ready`, `rdm.node_admin.fabric.update.via-state-entered`
(to ready) and `rdm.node_admin.fabric.update.via-on-fabric-ready`.

## Setting a state

A mesh's deleting is carried down and answered by member check-ins; a mesh's ready is
reported up by its mesh-primary once every planned birth is ready. A successful admission reply does
not set a state: the matching upward completions and the planned-birth checklist do. Required writes
finish before their completion check-ins.
Gossip disseminates the resulting states and overlays for peer topology and routing; it never
sets a state, and neither caller progress nor setting a state waits for peers to receive
gossip. Every writer writes its records straight to the store from Day 0: the Fabric record, each Mesh record and each Node record, on which a declared state acknowledged as Applied is durable before the reply; a successor acquires that fact without creating a new lifecycle transition.

| state | carried down | checked in | between |
|---|---|---|---|
| ready (fabric) | none | none | every mesh ready; one `on_fabric_ready` notice to Rafka |
| deleting (mesh) | mesh.delete | mesh.deleting | [mesh.delete](mesh-delete.md) |
| deleted (mesh) | none | mesh.deleted | [mesh.delete](mesh-delete.md) |

Each workflow's directed calls, stream events and gossip are on its page in the
[lifecycle index](../fabric-mesh-node-lifecycles.md); Build-backed work follows the
[Build carrier](../node-rpc-envelope.md#build-op-0x20-rdm).

## Routing

<a id="routing-stop"></a>
Application routing selects a birth only while it is ready and its mesh is
ready. There is no list of excluded states: anything else is a routing
stop, applied by a peer on receipt of the fact that says so. A node that is starting, pending,
connections-deleting, connections-deleted, stopping, stopped or deleting, a NodeStopped,
NodeDeleting or NodeDeleted overlay, a mesh deleting or deleted,
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
and leaves the live process stopped. node.get answers Current { stopped }.

node.start is the only transition out of stopped.
It retains node.id, incarnation, endpoint key, bound port and bin and completes all
start gates before ready. A stop then a start is not an exit and a new birth. node.delete proceeds from stopped, as deleting, to `TerminateRuntime`
and identity release, deleted. Silence decides nothing; node investigation does.

The [stopped-node collector](../node-identity-and-state.md#stopped-node-garbage-collector)
submits ordinary node.delete Builds after the configured stopped-node TTL, skipping
births referenced by open Builds. It performs no deletion itself. Blank TTL disables
the sweep; retained storage is not removed by collection.

## Leadership

Node status alone does not change a primary. Ordinary `node.connections.delete { scope: All }`
or node.stop of a primary requires [node.primary.update](node-primary-update.md) first
(node.fabric-primary.update for the fabric-primary).
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
- A primary taking its role reads the accepted Build from the fabric-primary (`FetchCurrentBuild`, 0x1F) before it counts its fresh checklist; the request is owed until answered, and the checklist is neither counted from facts held before the read nor completed while the read is unanswered (Q204, Q276).
- A member leaves the checklist only through the accepted removal or its re-creation,
  never by a timer.
- A check-in is a message the member owes its primary ([Node RPC §43](../node-rpc.md#43-owed-messages-delivery-is-the-runtimes),
  under `Budget::Owed`): delivered until the primary answers it, ended only by that answer, the
  primary's re-creation or accepted removal, or the member dropping it; re-owed to the new primary
  when the primary changes; never ended by a timer. The primary counts a check-in once per
  (state, birth) and answers a repeat `AlreadyApplied`.
- A primary counts only check-ins that arrive after it receives this state's command.

## Resize and executable replacement

A Build that changes only a node's `config`, `tags`, `storage` or `placement` is neither: it applies them as [build.md §1](../build.md#1-build-is-the-only-topology-mutation-engine) states, and the fabric stays ready.

A resize or an executable replacement puts the fabric back to pending; meshes already serving stay ready. Its Build is Complete only when the fabric is ready again from this Build's own change, as fabric.create's is: while the fabric changes, the Build stays in progress, never Complete. During an executable replacement, a member still on its old executable and identity does not satisfy its planned replacement. A new executable requires a replacement node with a new NodeId and incarnation ([build-deployment.md](../build-deployment.md)); a stop and a start retain the same process and executable. The mesh cannot report ready, and the fabric cannot change, until the replacement's last planned birth is ready.

See [the fabric's ready](#the-fabrics-ready) and [mesh.delete](mesh-delete.md).
