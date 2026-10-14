# Fabric state-commit

[Lifecycle index](../fabric-mesh-node-lifecycles.md) · [States](states.md) · [Op contracts](../node-rpc-envelope.md#the-op-ledger)

Fabric state-commit is the fabric state in which every node writes the records it holds in its
scratchpad into the store [state-sync](fabric-state-sync.md) established, carried down and
checked in as a round.

## What Rafka does with the states

- state-sync is Rafka's window: day 0 and seed per the [state-sync action table](fabric-state-sync.md#state-sync-actions).
- Rafka opens client traffic on the [ready-for-traffic](fabric-ready-for-traffic.md) notice.

### State-commit and opening traffic

1. Rafka answers the state-sync hook.
2. The fabric-primary sets the fabric's status to state-commit and carries it down in a
   round: fabric-primary → mesh-primaries → nodes. Each node writes what it holds in its
   scratchpad into the store the state-sync work established:
   - a node writes its connection rows;
   - a mesh primary writes its members' Node rows and its Mesh row;
   - the fabric-primary writes the Fabric row.
3. Each member checks in with its primary, node → mesh-primary → fabric-primary, under the
   [round checklist](states.md#the-round-checklist).
4. After all meshes check in, proceed to [Fabric ready-for-traffic](fabric-ready-for-traffic.md).

A broker's re-sync is not part of this round; [broker re-sync](/home/admin/rafka-v2/docs/architecture/broker-re-sync.md) states
when one runs.

## Command, completion and gossip messages

The round runs the [fabric round command pairs](../node-rpc-envelope.md#fabric-round-command-pairs)
under operation `commit-state:<fabric_id>`; each payload names its subject:

```text
fabric-primary → mesh-primary     CommitState { the mesh-primary }        ← Applied    hook StateCommitting
mesh-primary → planned member     CommitState { the member }              ← Applied    hook StateCommitting
member → mesh-primary             StateCommitted { the member }           ← Applied    hook StateCommitted
mesh-primary → fabric-primary     StateCommitted { the mesh-primary }     ← Applied    hook StateCommitted
```

Each line is its own directed call with its own target and caller span
([target and context](../node-rpc-envelope.md#target-and-context)); when a check-in is owed is the
[round checklist](states.md#the-round-checklist)'s. The hook frames' authors and channels are
[Fabric round gossip hooks](../gossip.md#fabric-round-gossip-hooks).

### Node-side and receiving-side trace evidence

Calls and service emit the [runtime spans](../node-rpc-envelope.md#runtime-spans); evidence
correlates admission, completion and the subject's action to the exact birth and operation.

### Round sequence diagram

```mermaid
sequenceDiagram
    participant F as Fabric-primary
    participant M as Mesh-primary
    participant N as Planned member birth
    participant G as Own mesh channel
    participant B as Backbone
    F->>M: commit-state (target + context + payload); command hook
    M->>N: commit-state (exact planned birth); command hook
    M->>G: state-committing
    M->>B: state-committing
    N->>N: complete required phase action
    N->>G: state-committed return hook
    N->>M: state-committed (own target + trace context)
    M->>B: carry member completion gossip
    M->>M: complete own action and all planned check-ins
    M->>F: state-committed; return hook
    F->>F: complete own action and every mesh check-in
```
