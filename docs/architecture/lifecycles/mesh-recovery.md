# Mesh recovery

[Lifecycle index](../fabric-mesh-node-lifecycles.md) · [States](states.md) · [Op contracts](../node-rpc-envelope.md#the-op-ledger)

Mesh recovery restores a mesh's primary after the node-admin that held the role is lost.
It is never a Build. It is the election, plus the steps the successor takes before it is the mesh's
writer, plus the recovery of the lost birth. While another node-admin of the mesh lives, that
node-admin is the successor and does everything below. The fabric-primary steps in only when the mesh
has no node-admin left. When the lost mesh-primary was also the fabric-primary, this page does not
apply: that is [fabric recovery](fabric-recovery.md#the-loss-of-the-fabric-primary-while-a-node-admin-lives).

## The loss of the mesh-primary while a node-admin of its mesh lives

1. **Unheard past the staleness floor.** Every node-admin of the mesh reads when it last heard the
   mesh-primary; unheard past the staleness floor (30 s at the default, `RDM_STALENESS_MS`), it marks it
   `PendingReconnect` ([node recovery](node-recovery.md) step 1). Silence is the trigger.
2. **The election names the successor:** the lowest Ready node-admin of the mesh
   ([elections §4.1](../fabric-node-lifecycle-elections.md#41-the-nodes-of-one-type-in-one-mesh)).
3. **The successor's first act is the backbone publish.** It publishes its mesh's aggregate
   (`TopologyFull`) on the backbone, naming itself mesh-primary, so the fabric-primary and every peer
   mesh hear the mesh's new primary before anything else moves. It publishes the update to its own
   mesh asynchronously, and records itself as the mesh's primary on its own row
   ([node.primary.update](node-primary-update.md)).
4. **The Build work its mesh owes.** The successor reads the current Build's events and runs each
   step its mesh owes that no `rdm.build.step` event ended
   ([build.md §4.1](../build.md#41-build-identity-is-active-reconciliation-state)).
5. **The lost birth is a member like any other.** The successor investigates it as any member of its
   mesh ([node investigation](node-investigation.md)), and the verdict decides (Q111). There are three cases:
   - **The investigation fails** (any verdict but `reached`):
     1. node.delete of the lost birth;
     2. a new birth at its path and name, a new node.id ([node recovery](node-recovery.md)), which
        starts with no primary role; nothing copies the predecessor's `is_primary` onto it;
     3. a previous node-admin that comes back after that is a deleted birth, as for any node
        ([node recovery](node-recovery.md)).
   - **The investigation succeeds** (`reached`), with no deletion:
     1. the lost node-admin removes its primary role: it publishes nothing as primary, and takes the
        mesh's primary from the successor's record;
     2. it rejoins the mesh through [node.mesh.join](node-mesh-join.md), commanded back by the
        successor, as an ordinary member.
   - **The successor hears no other node** and reads the lost birth `running`: the birth is
     unreachable from it, not lost. Nothing is written, no delete and no create, and the next
     investigation decides again (Q153).

## Two node-admins that each investigate the other

A node-admin that hears no other node-admin of its mesh elects itself and investigates the mesh-primary,
while the mesh-primary investigates it. Hearing no other node, its investigation writes nothing (Q153).
Once they hear each other, both run the one election over the
same ids and primary records and get the same answer ([elections §4.1](../fabric-node-lifecycle-elections.md#41-the-nodes-of-one-type-in-one-mesh)).
The command back, [node.mesh.join](node-mesh-join.md), carries the commander's held mesh-primary record,
which the commanded node-admin takes by the supersedes rule (the higher revision; equal, the lower node
id) before it runs its election, so both decide over the same records (D57). The one the election does
not name removes its primary role, its own held primary record included, and rejoins the mesh as an
ordinary member; named, it refuses the command by name (`RejectedNotPrimary { ReceiverNotPrimary }`).

## No node-admin of the mesh left

The fabric-primary watches each peer mesh's aggregate on the backbone, not the mesh-primary's birth.
When a mesh's aggregate stays unheard for twice the staleness floor (60 s at the default), no successor
took the role in the floor after the election, which is the mesh's own node-admin's to act in (Q335,
Q349). The fabric-primary then:

1. investigates the mesh's lost mesh-primary ([node investigation](node-investigation.md)), without
   the half-floor wait;
2. acts on the verdict, as its own workflow:
   - `reached`: it commands it back through [node.mesh.join](node-mesh-join.md). With no successor
     named, it keeps the mesh's primary role;
   - any other verdict: it re-creates the mesh's node-admin under the mesh's own MeshId
     ([a node-admin created into a live mesh](node-create.md#a-node-admin-created-into-a-live-mesh)).
     The re-created node-admin is the mesh's primary by the same election, and its first act is step 3
     above.

A mesh whose aggregate is heard again at any point cancels the fabric-primary's investigation.

## Command, completion and gossip messages

```text
successor (lowest Ready node-admin)  → backbone        TopologyFull, itself mesh-primary      (first)
successor                            → its own mesh    the update, asynchronously
successor                                              node.primary.update on its own row
successor                                              the current Build's owed steps, from its events
successor                            → lost birth      node investigation → node.mesh.join | node recovery
fabric-primary (aggregate unheard 2 × floor)    → lost birth   node investigation → node.mesh.join | node.create of a node-admin
```

Every call is sent under its `Send` (Q344); a recovery is never a Build (Q342). Each step has its span. Trace propagation and correlation follow the
[runtime spans](../node-rpc-envelope.md#runtime-spans).
