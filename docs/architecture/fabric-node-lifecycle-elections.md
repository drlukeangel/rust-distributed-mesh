# Node lifecycle elections — canonical architecture

[Lifecycle index](fabric-mesh-node-lifecycles.md) · [States](lifecycles/states.md) · [Op contracts](node-rpc-envelope.md#the-op-ledger)

This page owns the leadership election for the fabric-primary, each mesh-primary and each node-type
primary: the one rule, its inputs (the recorded primaries and the current member facts), the
staleness-floor rule, and the hand-over by [node.primary.update](lifecycles/node-primary-update.md).
Other pages consume the elected primary and link here.

RDM's `rdm-admin` implements the one election; Rafka binds its facts to it and consumes the
elected primaries.

---

## 1. Election is a deterministic projection, not a voting protocol

There are no election ballots, terms, coordinator messages, quorum votes, or incumbent leases.

~~~text
the recorded primary (`is_primary` on the node rows) + current candidate facts
    -> the primary stays primary while it is heard
    -> unheard past the staleness floor, or no primary: eligible candidates of that type in that mesh
    -> lowest complete NodeId
    -> the elected primary
~~~

Equal candidate facts and equal records produce equal winners. Every primary moves in two ways only: it
hands over by [node.primary.update](lifecycles/node-primary-update.md) (`node.fabric-primary.update` for the
fabric-primary), or it is unheard past the staleness floor and is superseded by the rule above (Q107 (1),
Q89). Nothing else moves a primary, and nothing waits for proof of anything (Q111).

A published or recorded `is_primary` or `is_fabric_primary` bit is a projection of this result. It
never makes a primary by advertisement or by a write: the write records the role.

A peer mesh that goes unheard may temporarily produce different local candidate sets. A failed RPC, a
failed dial, gossip silence or an unheard peer mesh does not itself say what happened to a candidate and
does not license destructive recovery: [node investigation](lifecycles/node-recovery.md) decides that
(Q111).

---

## 2. Eligibility

For ordinary election/recomputation:

~~~text
eligible = current MemberStatus::Ready
~~~

Eligibility is current Ready and nothing else.

How a node becomes Ready is lifecycle policy, not election policy. A node-admin must satisfy
the hydration gates owned by [build.md](build.md) before lifecycle makes it Ready.

A node that is starting, pending, connections-deleting, connections-deleted, stopping, stopped or
deleting, and deleted births, are not new ordinary election candidates.

Ready time is not priority.

---

## 3. Election key

A primary stays primary while it is heard. Its record — its exact birth `(mesh, node_id, incarnation)`
and its revision, `is_primary` on its row — is an election input: it is gossiped when a primary changes
and handed to a joiner in its entry. When the primary is unheard past the staleness floor, or there is
none, the winner among eligible candidates is the **lowest complete canonical NodeId**.

NodeId is the fixed-width 12-character lowercase Crockford60 product identity. Compare the complete value.

Not election inputs:

~~~text
ready_since
wall clock / creation time
path.name / ordinal
MeshId
FabricId
EndpointId
IncarnationId
DeploymentId
endpoint slot/freshness
RuntimeFact
provider locator/control domain
current Build
Build attempt
~~~

Restart of the same logical node preserves NodeId. Permanent logical replacement mints a new NodeId, so
replacement does not inherit election rank. One exception, [fabric recovery](lifecycles/fabric-recovery.md)
(Q122): while it restores the fabric-primary, no other node becomes fabric-primary or node-admin primary
of that mesh; the fabric-primary stays assigned to the recovery target's path.name, and when the node
re-created there is Ready the workflow restores it as mesh-primary and fabric-primary. The
recovery attempt supplies the target; nothing about a lower id or a matching name displaces a primary
anywhere else.

---

## 4. Election hierarchy

### 4.1 The nodes of one type in one mesh

For the nodes of every type in every mesh, the one election applies the same eligibility/key rule. A
node's type is its `node_type` field ([node identity](node-identity-and-state.md)), never its name. That
type's primary in that mesh is the node-type primary, and that node's `is_primary`, which the
mesh-primary records on its row.

Ordinary nodes publish current facts. They do not author election outcomes.

A primary unheard past the staleness floor is superseded by the lowest Ready node of its type
in its mesh; the mesh-primary announces and records it ([node.primary.update](lifecycles/node-primary-update.md)).
For a node-admin the questions are, in order: am I a node-admin; am I the lowest Ready one;
was the former mesh-primary also the fabric-primary? No: I am mesh-primary now. Yes: I take no primary
role and run [fabric recovery](lifecycles/fabric-recovery.md) (Q122).
The mesh-primary then runs [node investigation](lifecycles/node-recovery.md) on the unheard node, whatever
its type, and that verdict decides what becomes of it (Q111): reached, it is commanded back as a member;
anything else, it is re-created.

### 4.2 Mesh primary

~~~text
(mesh, `node-admin`) winner == mesh primary
~~~

There is no second mesh-primary election. The mesh-primary writes the mesh's rows.

### 4.3 Fabric primary

The fabric-primary is the mesh-primary of one mesh, an explicit property of that node-admin
(`is_fabric_primary`, [node.primary.update](lifecycles/node-primary-update.md)). The mesh-primary and the fabric-primary move
together, under the same rule: lowest complete NodeId, a heard primary kept.

~~~text
Ready node-admins of the fabric-primary's mesh
    -> same election rule
    -> mesh primary, and fabric primary with it
~~~

There is no MeshId/name/age scoring and no separate fabric-election algorithm. The fabric-primary hands
over by [node.fabric-primary.update](lifecycles/node-primary-update.md#the-fabric-primary-nodefabric-primaryupdate);
nothing else moves it. Unheard past the staleness floor, nobody takes it: the lowest Ready
node-admin of its mesh, or when the mesh has none the lowest Ready mesh-primary on the
backbone from [mesh investigation](lifecycles/mesh-investigation.md)'s decision step, runs
[fabric recovery](lifecycles/fabric-recovery.md#the-loss-of-the-fabric-primary-while-a-node-admin-lives),
the one mechanism in either case (Q122): the node-admin at the fabric-primary's path.name is re-created
and is mesh-primary and fabric-primary again when it is Ready. The fabric is `degraded`
meanwhile.

Only the elected fabric-primary may accept a Fabric topology change. The election chooses **who may
write topology**; [the current Build](build.md#4-the-accepted-topology-is-the-fabric-primarys-current-build)
defines **what topology is accepted**.

---

<a id="44-explicit-committed-fabric-handover-r-a2--r-st2"></a>
<a id="44-explicit-committed-fabric-handover"></a>

### 4.4 The commanded hand-over of the fabric-primary

[node.fabric-primary.update](lifecycles/node-primary-update.md#the-fabric-primary-nodefabric-primaryupdate)
moves the fabric-primary while the incumbent mesh's node-admins remain alive; its selection is this page's
election, with the departing mesh excluded under mesh.delete, or the successor the fabric-primary names
(the node at the path.name that lost it, returned or re-created, §4.3). The next-revision row is the
election's input: under mesh.delete the incumbent stays mesh-primary and is fabric-primary no more. The
hand-over needs no incumbent-mesh death, infers nothing from silence and turns no announcement into a
fact.

A Build that runs `node.connection.delete { scope: All }` on, stops or replaces a primary
follows the [node.primary.update](lifecycles/node-primary-update.md) sequence: the primary, commanded
from above, calls node.primary.update on its successor; the successor becomes primary and, for the
`node-admin` type, the mesh's writer from that moment, and records the change itself. The record is the
same election input as any other, and the old primary writes the mesh's rows no more from the moment
the successor is mesh-primary.

## 5. Day 0, preemption, restart, and replacement

Day 0 ([fabric.create](lifecycles/fabric-create.md)) is not a special primary assignment. The first Ready
node-admin is simply the only eligible node-admin, so the ordinary rule makes it the initial
mesh-primary and fabric-primary.

For every primary:
- a heard primary stays primary; a lower-NodeId candidate that later becomes Ready never displaces it;
- silence past the staleness floor supersedes it (§4.1, §4.3); nothing is terminated on silence. What
  became of the unheard node is [node investigation](lifecycles/node-recovery.md)'s verdict (Q111): reached,
  it is commanded back as a member through node.mesh.join; anything else, it is re-created at its
  path.name with a new node.id (Q110), and an old birth that comes back is refused at admission and its
  runtime terminated;
- a primary that hands over gives the primary to the lowest Ready NodeId of its type in its
  mesh;
- restart preserves NodeId; a restarted node is a candidate for a later vacancy and never displaces the
  primary;
- replacement mints a new NodeId and may or may not win.

Election movement never changes the current Build, never changes accepted topology, and never mints a
Build: the current accepted Build is read LIFO from `builds.storage` (R-BI3, R-BI4).

---

<a id="6-fabric-shutdown-does-not-invent-another-election"></a>

## 6. fabric.delete moves no primary

A primary moves only by its own hand-over or the staleness-floor rule, never by a status (§5), so
[fabric.delete](lifecycles/fabric-delete.md) moves no primary: each mesh-primary stays mesh-primary until
it is itself deleted, and the fabric-primary stays fabric-primary to the end.

---

## 7. Publication and consumption

Election inputs travel through current topology dissemination. The elected result may also be
published for fast consumption.

Consumers do not reconstruct the algorithm. They consume the elected primary.

~~~text
WRONG:
    X advertised primary
    -> trust X

WRONG:
    product protocol reads candidates
    -> runs its own comparator

RIGHT:
    the one election elects the primary
    -> consumer uses the elected primary
~~~

Any architecture document that needs "current mesh primary" or "current fabric primary" should link this
document and consume that elected primary.

---

## 8. Unheard peers and recovery

Election has no separate repair protocol.

Held-member repair, exact runtime/lifecycle facts, and control-state hydration restore the candidate
view. When observers again hold the same current candidate facts, the election returns the same winner.

A failed join, failed RPC, failed dial, silence, or provider-domain mismatch is not election evidence
that a candidate is gone.

---

## 9. Required invariants

1. This file is the only normative election specification.
2. One election implementation exists.
3. Ordinary election eligibility is current Ready.
4. A heard primary stays primary; for a vacancy the complete canonical NodeId is the only ordering key.
5. The `node-admin` winner of a mesh and its mesh-primary are one.
6. The fabric-primary is a mesh-primary; its candidates are its mesh's node-admins, else the
   mesh-primaries on the backbone.
7. Primary advertisements are projections, never a fact of their own.
8. Equal candidate sets and equal records produce equal winners.
9. Day 0 uses the ordinary rule.
10. Restart preserves NodeId; permanent logical replacement does not.
11. Election movement never changes accepted topology or mints a Build.
12. Failed reachability decides nothing; node investigation does.
13. A primary moves only by its own hand-over or the staleness-floor rule, never by a status;
    fabric.delete moves none.
14. Rafka product/domain code consumes the elected primary; it never keeps a second comparator or
    election state machine.

---

## 10. Implementation boundary

Conceptually, for the nodes of one type in one mesh:

~~~rust
fn elect<'a>(primary: Option<&Primary>, candidates: &'a [Node]) -> Option<&'a Node> {
    if let Some(p) = primary {
        match candidates.iter().find(|n| n.node_id == p.node_id) {
            _ if p.unheard_past_floor() => {}                              // unheard past the floor: a vacancy
            None => return None,                                           // primary not yet heard: it waits
            Some(n) if n.incarnation == p.incarnation => return Some(n),   // a heard primary stays primary
            Some(_) => {}                                                  // a later birth replaced it: a vacancy
        }
    }
    candidates
        .iter()
        .filter(|n| primary.map_or(true, |p| n.mesh == p.mesh))
        .filter(|n| n.status == MemberStatus::Ready)
        .min_by_key(|n| n.node_id)
}
~~~

Exact Rust names may differ. The contract does not:

~~~text
the recorded primary, heard -> that primary
the recorded primary, not yet heard -> it waits
vacant (handed over, unheard past the floor, or no record)
       -> current Ready candidates of that type in that mesh
       -> lowest complete NodeId
       -> one elected primary
the node-admin type of a mesh whose unheard mesh-primary was also the fabric-primary (Q122)
       -> no node-admin is elected: the lowest Ready one runs fabric recovery instead;
          the fabric-primary stays assigned to the target's path.name, and the workflow restores the
          node re-created there as mesh-primary and fabric-primary when it is Ready;
          the mesh's other types elect as above
~~~

RDM owns the election. Rafka binds facts and consumes the result. No downstream document or
implementation gets another election algorithm.
