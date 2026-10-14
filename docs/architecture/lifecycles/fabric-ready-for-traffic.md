# Fabric ready-for-traffic

[Lifecycle index](../fabric-mesh-node-lifecycles.md) · [States](states.md) · [Op contracts](../node-rpc-envelope.md#the-op-ledger)

Fabric ready-for-traffic is the round that opens traffic. When every mesh has checked in for
[state-commit](fabric-state-commit.md), the fabric-primary carries the ready-for-traffic status
down in a round: fabric-primary → mesh-primaries → nodes. Once every member has checked in under
the [round checklist](states.md#the-round-checklist), the fabric is ready-for-traffic and Rafka
receives one notice, on which it opens client traffic.

## Command, completion and gossip messages

The round runs the [fabric round command pairs](../node-rpc-envelope.md#fabric-round-command-pairs)
under operation `open-traffic:<fabric_id>`; each payload names its subject:

```text
fabric-primary → mesh-primary     OpenTraffic { the mesh-primary }       ← Applied    hook TrafficOpening
mesh-primary → planned member     OpenTraffic { the member }             ← Applied    hook TrafficOpening
member → mesh-primary             TrafficOpened { the member }           ← Applied    hook TrafficOpened
mesh-primary → fabric-primary     TrafficOpened { the mesh-primary }     ← Applied    hook TrafficOpened
fabric-primary                    FabricStatus ready-for-traffic; one ready-for-traffic notice to Rafka
```

Each line is its own directed call with its own fence and caller span
([target and context](../node-rpc-envelope.md#target-and-context)); when a check-in is owed is the
[round checklist](states.md#the-round-checklist)'s. The hook frames' authors and channels are
[Fabric round gossip hooks](../gossip.md#fabric-round-gossip-hooks).

FabricStatus ready-for-traffic travels under the [frame catalog](../gossip.md#31-frame-catalog);
it does not replace the round checklist. Who declares the transition is [States](states.md#states)'s.

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
    F->>M: open-traffic (fence + context + payload); command hook
    M->>N: open-traffic (exact planned birth); command hook
    M->>G: traffic-opening
    M->>B: traffic-opening
    N->>N: complete required phase action
    N->>G: traffic-opened return hook
    N->>M: traffic-opened (own fence + trace context)
    M->>B: carry member completion gossip
    M->>M: complete own action and all planned check-ins
    M->>F: traffic-opened; return hook
    F->>F: complete own action and every mesh check-in
```
