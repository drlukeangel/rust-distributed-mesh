# Fabric create

[Lifecycle index](../fabric-mesh-node-lifecycles.md) · [States](states.md) · [Typed calls and wire mapping](../node-rpc-envelope.md#typed-object-contract)

`fabric.create(spec)` brings a fabric into being with its first mesh. The fabric id is minted with
status pending, each mesh's first node-admin is created, the first mesh's primary becomes the
fabric-primary, and the fabric changes to ready-for-traffic. This page owns that sequence; the
page that owns each step is linked at the step.

## Operation contract

| Field | Contract |
|---|---|
| Caller / executor | A person starts the first node-admin with no mesh and no durable topology ([start modes](../node-admin-lifecycle.md#3-start-modes)); that node-admin is the executor and the fabric-primary |
| Entry | No fabric exists; Day 0 is the first node-admin and its own mesh, nothing else. Every further mesh, and every other node, is created by a Build accepted after the fabric's first ready-for-traffic (Q170) |
| Identity order | The fabric id is minted first, each mesh with that fabric id, each node with its mesh id ([identity creation order](../fabric-primary.md#11-identity-creation-order)); the bootstrap admin's own node id is minted before its mesh's, by the bootstrap's first step, before anything else of its boot (Q208) |
| Primaries | The first node-admin is the only eligible candidate and is both mesh-primary and fabric-primary ([elections](../fabric-node-lifecycle-elections.md#5-day-0-preemption-restart-and-replacement)) |
| Rows | The fabric-primary creates the fabric row and its own mesh's row at Day 0, written to the store root at once ([fabric entity](../node-identity-and-state.md#28-a-fabric-is-an-entity-and-the-fabric-primary-owns-it)) |
| Status | pending, then ready-for-traffic ([States](states.md#setting-a-state)) |
| Completion | `fabric.created` when the fabric is ready-for-traffic |
| Wire | [Typed-object mapping](../node-rpc-envelope.md#typed-object-contract); the accepted topology is carried by the [Build carrier](../node-rpc-envelope.md#build-op-0x20-rdm) |

## Numbered end-to-end sequence

1. Mint the fabric id; its status is pending. `fabric.root.create`: the bootstrap admin takes the fabric
   root from `CertChoice::Ca` or, with no seeds and no anchor, mints one; `fabric.ca.create`: it mints RDM's
   CA under that root ([fabric certs](../fabric-certs.md#the-cert-workflows)).
2. [node.create](node-create.md) of the first node-admin, whose own mesh comes into being with it
   ([mesh absent](node-create.md#mesh-absent)). Every further mesh comes into being with its first
   node-admin, created by a Build after the fabric's first ready-for-traffic (Q170).
3. The first mesh's first node-admin is the first fabric-primary. It is both mesh-primary and fabric-primary, accepts the
   first Build ([ready root](../build.md#101-ready-root--deadlock-guard)),
   and creates the fabric row and its own mesh's row. Every other mesh's row is created by that
   mesh's primary.
4. When every mesh the accepted Build names is ready-for-traffic, the fabric-primary sets the fabric
   [ready-for-traffic](states.md#the-fabrics-ready) (Q174).
5. The application is told once (`on_fabric_ready`) and opens client traffic.
6. Emit `fabric.created`. The call ends here.

The statuses are set only by the fabric-primary and carried down, fabric-primary to
mesh-primaries to nodes; the check-in checklist is [States](states.md#the-check-in-checklist).

## Command and completion messages

```text
fabric.create
  → mint fabric id (pending)
  → node.create for each mesh's first node-admin (every FirstStart event, node.created)
  → states: ready-for-traffic
  ← fabric.created
```

## Mermaid sequence diagram

```mermaid
sequenceDiagram
    participant P as Person
    participant F as First node-admin (fabric-primary)
    participant M as Other mesh's first node-admin
    P->>F: start with no mesh, no durable topology
    Note over F: mint fabric id (pending), create fabric row and own mesh row
    F->>M: node.create (first node-admin of each other mesh)
    M-->>F: node.created
    Note over F: every mesh ready-for-traffic
    Note over F: ready-for-traffic; on_fabric_ready
    F-->>P: fabric.created
```

## Failure and receipts

Failures and stream certainty follow the
[caller execution contract](../node-rpc-envelope.md#caller-execution-and-certainty).

## Observability contract

Trace propagation, step spans and correlation follow the
[runtime spans](../node-rpc-envelope.md#runtime-spans).
