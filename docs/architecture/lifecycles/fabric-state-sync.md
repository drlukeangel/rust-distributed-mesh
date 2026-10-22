# Fabric state-sync

[Lifecycle index](../fabric-mesh-node-lifecycles.md) · [States](states.md) · [Op contracts](../node-rpc-envelope.md#the-op-ledger)

Fabric state-sync is the fabric state in which the application does its day-0 and seed
work, handed over and answered through the [`sync_state`](../fabric-node-hooks.md#sync_state)
hook. RDM writes none of its own records during state-sync; they wait in the scratchpad for
[state-commit](fabric-state-commit.md), except a node's declared state acknowledged as Applied and
the Build facts (each accepted Build, attempt claim and receipt), which are written to the
shared store root when they are committed ([build.md §5.2](../build.md#52-rafka-adapter)). Rafka's application writes establish the system
org and seed; those writes are not RDM's fabric-record publication. Every fabric runs
day 0, by script or by attaching its saved WAL image. State-sync passes through
day0-complete and seed-complete as the requested application work completes.

<a id="state-sync-actions"></a>

| goal | action |
|---|---|
| day 0, unchanged | attach-system-image |
| day 0, changed | run-day0 → save-system-image |
| seed, unchanged | attach-golden-image |
| seed, changed | attach-system-image → run-seed → save-golden-image |

"unchanged" means the phase's content key has a saved image; "changed" means it has none.
These actions are application work inside the fabric's state-sync. The system image is
the WAL at day0-complete; the golden image is the WAL at seed-complete. A test of the
day-0 code forces the "day 0, changed" path. Rafka answers the state-sync hook when its
work ends; that answer starts state-commit, not ready-for-traffic.

## Command, completion and gossip messages

The [application handoff](../node-rpc-envelope.md#application-state-sync-handoff)
is external integration, not a defined Node RPC family. These message values show
context and round correlation without inventing a target or opcode for an unspecified
application transport. RDM owns the lifecycle; Rafka owns the state-sync work.

### State-sync handoff → application

```json
{
  "message": "sync-state",
  "fabric_id": "fabric1",
  "build_id": "bld_1a2b3c4d",
  "attempt": 1,
  "operation": "state-sync:fabric1",
  "context": {
    "caller_system": "rdm",
    "traceparent": "00-f16a8b75a1b2c3d4e5f60718293a4b5c-1111111111111111-01",
    "tracestate": null,
    "baggage": "operation=example"
  }
}
```
### Application → fabric-primary: the state-sync hook's answer

```json
{
  "message": "state-synced",
  "fabric_id": "fabric1",
  "build_id": "bld_1a2b3c4d",
  "attempt": 1,
  "operation": "state-sync:fabric1",
  "context": {
    "caller_system": "rdm",
    "traceparent": "00-f16a8b75a1b2c3d4e5f60718293a4b5c-1111111111111111-01",
    "tracestate": null,
    "baggage": "operation=example"
  }
}
```
The hook's answer means the requested application work is complete for this round. It
starts [state-commit](fabric-state-commit.md), not traffic opening. A foreign Build,
old attempt or incomplete application phase is not this completion.

### Fabric state dissemination

The fabric-primary publishes the entered status, state-sync, as FabricStatus under the
[frame catalog](../gossip.md#31-frame-catalog). No Rafka day-0/seed content is
put into a membership frame or treated as node/mesh state.

### Sequence diagram and evidence

```mermaid
sequenceDiagram
    participant F as Fabric-primary
    participant A as Rafka application
    participant B as Backbone
    F->>F: all planned meshes ready; enter state-sync
    F->>B: FabricStatus state-sync
    F->>A: state-sync hook (round + trace context)
    A->>A: requested day-0/seed work
    A->>F: hook answer (matching round)
    F->>F: validate completion; begin state-commit
```

Evidence must connect the application handoff, completed requested work and the matching
hook answer to the accepted Build/attempt and trace. A generic status publication
alone proves none of the application writes.
