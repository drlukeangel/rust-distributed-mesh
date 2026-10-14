# Node lifecycle elections — canonical architecture

[Lifecycle index](fabric-mesh-node-lifecycles.md) · [States](lifecycles/states.md) · [Op contracts](node-rpc-envelope.md#the-op-ledger)

This page owns elections for fabric, mesh and node leadership: the seats, the sticky holder, the
seat watch and the committed seat records the resolver takes as input. Other pages consume the
resolved primary and link here.

RDM's `rafka-node-admin-core` implements the one LeadershipResolver; Rafka binds its facts and projections to it and consumes the resolved leadership view.

---

## 1. Election is a deterministic projection, not a voting protocol

There are no election ballots, terms, coordinator messages, quorum votes, or incumbent leases.

~~~text
held seat record + current candidate facts
    -> the holder keeps the seat while it holds it
    -> on a vacancy: eligible candidates of the holder's mesh
    -> lowest complete NodeId
    -> resolved primary
~~~

Equal candidate facts and equal seat records produce equal winners.

A published is_primary or is_fabric_primary bit is a projection of this result. It never creates authority by advertisement.

A peer mesh that goes unheard may temporarily produce different local candidate sets. A failed RPC, a failed dial, gossip silence or an unheard peer mesh does not itself prove a candidate dead and does not license destructive recovery. Membership/runtime recovery owns convergence and exact-death proof.

---

## 2. Eligibility

For ordinary election/recomputation:

~~~text
eligible = current MemberStatus::ReadyForTraffic
~~~

Eligibility is current ReadyForTraffic and nothing else.

How a node becomes ReadyForTraffic is lifecycle policy, not election policy. A node-admin must satisfy the authority-capable hydration gates owned by [build.md](build.md) before lifecycle makes it Ready.

A node that is starting, pending, connections-deleting, connections-deleted, stopping, stopped or deleting, and deleted births, are not new ordinary election candidates.

Ready time is not priority.

---

## 3. Election key

A seat stays with its holder until that holder is proven unable to hold it. The
holder record — the seat's exact birth `(mesh, node_id, incarnation)` and its revision — is an
election input: it is gossiped when a seat changes hands and handed to a joiner in its entry.
When a seat is vacant, the winner among eligible candidates is the **lowest complete canonical
NodeId**.

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
Fabric.build_id
Build attempt
~~~

Restart of the same logical node preserves NodeId. Permanent logical replacement mints a new NodeId, so replacement does not inherit election rank.

---

## 4. Election hierarchy

### 4.1 In-mesh cohorts

For every (mesh, node_type) cohort, the one imported resolver applies the same eligibility/key rule. A node's type is its `node_type` field ([node identity](node-identity-and-state.md)), never its name. Each cohort's primary is that node's `is_primary`.

Ordinary nodes publish current facts. They do not author election outcomes.

A node-admin investigating a silent seat holder runs node recovery's
[tickle ladder](lifecycles/node-recovery.md#numbered-end-to-end-sequence). Only the provider's
exit proof vacates a seat; no probe outcome does.

### 4.2 Mesh primary

~~~text
(mesh, `node-admin`) cohort winner == mesh primary
~~~

There is no second mesh-primary election.

### 4.3 Fabric primary

The fabric-primary candidate set is the set of current mesh-primary node-admins.

~~~text
current mesh primaries
    -> same election rule
    -> fabric primary
~~~

There is no MeshId/name/age scoring and no separate fabric-election algorithm. The fabric seat
stays in its holder's mesh: when its holder yields or its exact birth is proven gone, the
lowest ReadyForTraffic node-admin NodeId of the same mesh fills it, and if only Pending admins
survive the seat waits. It leaves that mesh only when every node-admin birth of the mesh is
proven gone or has left; then the lowest NodeId among the remaining mesh primaries wins and its
mesh holds the seat. Nothing reclaims the seat for the mesh that lost it.

Only the resolved fabric-primary may accept a Fabric topology change. The election chooses **who may write topology**; [Fabric.build_id -> Build](build.md) defines **what topology is accepted**.

---

<a id="44-explicit-committed-fabric-handover-r-a2--r-st2"></a>

### 4.4 Explicit committed fabric handover

[node.fabric-primary.update](lifecycles/node-primary-update.md#fabric-seat) commits a transfer of the fabric
seat to an outside mesh-primary while the incumbent mesh's node-admins remain alive; its
selection is this page's leadership calculation with the departing mesh excluded. The committed
next-revision seat record is authoritative input to the resolver: the incumbent keeps its
mesh-primary seat and yields fabric authority. The transfer needs no incumbent-mesh death,
infers no death from silence and turns no announcement into authority.

A Build that runs `node.connections.delete { scope: All }` on, stops, restarts or replaces a seat holder
follows the [node.primary.update](lifecycles/node-primary-update.md) sequence: the holder, commanded from
above, writes the one committed seat row, then calls node.primary.update on its successor. The holder's
one committed seat row is the same authoritative resolver input as the handover record, and the holder's
execution authority ends at the row's revision.

## 5. Day 0, preemption, restart, and replacement

Day 0 ([fabric.create](lifecycles/fabric-create.md)) is not a special primary assignment. The first Ready node-admin is simply the only eligible node-admin, so the ordinary rule yields the initial mesh-primary and fabric-primary seats.

For every seat:
- a living holder keeps its seat; a lower-NodeId candidate that later becomes Ready never displaces it;
- silence, an observer-inferred Dead or an unheard route is not proof: the holder keeps the seat;
- a holder that yields or whose exact birth is proven gone hands the seat to the lowest ReadyForTraffic NodeId of its own mesh;
- restart preserves NodeId; a restarted node is a candidate for a later vacancy and never displaces the holder;
- replacement mints a new NodeId and may or may not win.

Election movement never changes Fabric.build_id, never changes accepted topology, and never mints a Build.

---

<a id="6-fabric-shutdown-does-not-invent-another-election"></a>

## 6. fabric.delete moves no seat

A seat moves only by its holder's yield, a committed transfer (§4.4) or a proven loss, never by a
status (§5), so [fabric.delete](lifecycles/fabric-delete.md) moves no seat: each mesh-primary keeps
its seat until it is itself deleted, and the fabric-primary keeps the fabric seat to the end.

---

## 7. Publication and consumption

Election inputs travel through current membership/topology dissemination. The resolved result may also be published for fast consumption.

Consumers do not reconstruct the algorithm. They consume the imported leadership view.

~~~text
WRONG:
    X advertised primary
    -> trust X

WRONG:
    product protocol reads candidates
    -> runs its own comparator

RIGHT:
    imported LeadershipResolver resolves primary
    -> consumer uses LeadershipView
~~~

Any architecture document that needs "current mesh primary" or "current fabric primary" should link this document and consume that resolved view.

---

## 8. Unheard peers and recovery

Election has no separate repair protocol.

Held-member repair, exact runtime/lifecycle facts, and control-state hydration restore the candidate view. When observers again hold the same current candidate facts, the resolver returns the same winner.

A failed join, failed RPC, failed dial, silence, or provider-domain mismatch is not election evidence that a candidate is gone.

---

## 9. Required invariants

1. This file is the only normative election specification.
2. One imported LeadershipResolver implementation exists.
3. Ordinary election eligibility is current ReadyForTraffic.
4. A held seat stays with its holder; for a vacancy the complete canonical NodeId is the only ordering key.
5. Node-admin cohort winner and mesh primary are one seat.
6. Fabric-primary candidates are current mesh-primary node-admins.
7. Primary advertisements are projections, never independent authority.
8. Equal candidate sets and equal seat records yield equal winners.
9. Day 0 uses the ordinary rule.
10. Restart preserves NodeId; permanent logical replacement does not.
11. Election movement never changes accepted topology or mints a Build.
12. Failed reachability is never destructive death proof.
13. A seat moves only by its holder's yield, a committed transfer or a proven loss, never by a status; fabric.delete moves none.
14. Rafka product/domain code consumes LeadershipView; it never keeps a second comparator or election state machine.

---

## 10. Implementation boundary

Conceptually, for one seat:

~~~rust
fn elect<'a>(holder: Option<&SeatHolder>, candidates: &'a [Node]) -> Option<&'a Node> {
    if let Some(h) = holder {
        match candidates.iter().find(|n| n.node_id == h.node_id) {
            _ if h.is_proven_gone() => {}                                // proven gone: a vacancy
            None => return None,                                         // holder not yet heard: the seat waits
            Some(n) if n.incarnation == h.incarnation => return Some(n), // a living holder keeps its seat
            Some(_) => {}                                                // a later birth replaced it: a vacancy
        }
    }
    candidates
        .iter()
        .filter(|n| holder.map_or(true, |h| n.mesh == h.mesh))
        .filter(|n| n.status == MemberStatus::ReadyForTraffic)
        .min_by_key(|n| n.node_id)
}
~~~

Exact Rust names may differ. The contract does not:

~~~text
held seat record, holder living -> that holder
held seat record, holder not yet heard -> the seat waits for it
vacant (holder yielded or proven gone, or no record)
       -> current ReadyForTraffic candidates of the holder's mesh
       -> lowest complete NodeId
       -> one resolved primary
~~~

RDM owns the resolver. Rafka binds facts and consumes the result. No downstream document or implementation gets another election algorithm.
