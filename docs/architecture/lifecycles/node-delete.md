# Node delete

[Lifecycle index](../fabric-mesh-node-lifecycles.md) · [States](states.md) · [Typed calls and wire mapping](../node-rpc-envelope.md#typed-object-contract)

`node.delete` composes the whole [node.stop](node-stop.md), then terminates the exact runtime
through its provider, reads it `Exited`, and releases identity. It releases node.id and name, **not storage**. The data
dir and node-identity.json / endpoint key stay with that data dir. Application storage
removal is separate work.

## Operation contract

| Field | Contract |
|---|---|
| Caller / executor | Typed caller / accepted node-admin executor |
| Live target | Complete node.stop first; the stopped process remains reachable |
| Crashed target | Stop steps are already done; no call is required from an exited process |
| OS-suspended target | The provider reports the exact runtime `os-stopped` ([deployment-provider §2.5](../deployment-provider.md#25-runtime-state-what-the-provider-reports)): the stop is skipped by name (a suspended process answers no call) and the delete begins at `TerminateRuntime` on the exact handle, as `terminate(Immediate)`: a suspended runtime runs no handler, so there is no graceful wait, and nothing of it is resumed; the delete completes only on the provider's `Exited`. Explicit delete only, never recovery, which respects suspension |
| Runtime action | `TerminateRuntime` on the exact accepted runtime, whatever the provider reads of it (a running one is stopped; an exited one has what it left removed; one already removed is done), then its inspection (Q145) |
| Terminal event | Executor emits node.deleted only from the provider's `Exited` of the exact runtime, after releasing node.id and name |
| Storage | Retain the data dir; a later node at the path can inherit it, never its endpoint key (Q358) |
| Peer block | node.deleted blocks old `(node.id, incarnation)` in the peer-location view |
| Never evidence | Gossip, silence, Ping failure and timeout never say the runtime exited |

## Numbered end-to-end sequence

1. For a live process compose the whole [node.stop](node-stop.md#numbered-end-to-end-sequence).
   For a crashed process those stop steps are already done; do not fabricate node replies or
   receipts.
2. Terminate and inspect the exact runtime through provider control. Match its immutable
   runtime identity; a recycled pid or a reusable container name is not the runtime.
3. The node's mesh-primary writes the node's record with state deleted; the record and its config stay. Release node.id
   and the name. Retain the data dir, node-identity.json and endpoint key.
4. The surviving executor emits node.deleted on the caller stream from the provider's
   `Exited` and writes the delete's `rdm.build.step` event (inside a repair, the node record). The exited node cannot publish
   its own end or serve that stream.
5. Block the old `(node.id, incarnation)` in the peer-location view. Peers refuse a late
   digest of the deleted birth under the [frame catalog's receiver rules](../gossip.md#31-frame-catalog).

## Command, completion and gossip messages

```text
node.delete
  ← every node.stop event, if the process is live
  ← node.deleted [executor: the provider's Exited of the exact runtime; node.id and name released]
```

The executor, not the exited node, publishes the last event.
[NodeDeleting / NodeDeleted](../gossip.md#31-frame-catalog) are peer dissemination;
node.deleted is the caller event. The caller waits on the stream and the Build's events.

## Exact runtime exit evidence

Stop's node.stopped is pre-exit (stopped) and insufficient. Retain the exact accepted RuntimeFact
and reach node.deleted and identity release through the delete's own steps, `TerminateRuntime`
included. `TerminateRuntime` is never skipped because the runtime already exited: the provider's
terminate of an exited runtime removes what it left (a container and the address it held, its logs
kept beside the data dir first; a process leaves nothing), and a runtime already removed terminates as
done, so a re-run of the step succeeds (Q145). The one exception is fabric recovery's backbone arm, which terminates nothing in a mesh it is outside of ([fabric recovery](fabric-recovery.md) step 3). An unreachable provider leaves delete incomplete. A crashed node is
[node investigation](node-investigation.md)'s verdict; lack of a reply never licenses deletion. The runtime-exit responsibility
is owned here and is composed by [node recovery](node-recovery.md), the primary's own workflow,
never a Build.

## Mermaid sequence diagram

```mermaid
sequenceDiagram
    participant C as Caller
    participant F as Fabric-primary
    participant M as Executor
    C->>F: build.create(Delete { node_ids })
    F->>M: build.section.run (the mesh's section)
    Note over M: writes the node's deleting
    M-->>F: node.stop events for a live process
    Note over M: the provider's Exited of the exact runtime; writes the node's deleted
    M-->>F: node.deleted
    F-->>C: step frames, Complete
```

## States written

The node's mesh-primary writes these on the node's record, a `node-record` line in `nodes.jsonl`, whole (the fabric-primary for a mesh with no primary). The executor writes each step of the Build as an `rdm.build.step` line in `events.jsonl`. The stop inside the delete writes [node.stop](node-stop.md)'s states.

| written | by | when |
|---|---|---|
| `deleting` | the node's mesh-primary | node.delete's admission |
| `deleted` | the node's mesh-primary | the provider's Exited of the exact runtime |
| `rdm.build.step`, `state` `complete` | the executor | the step completes |

As stored:

`deleting`:

```json
{"len":1386,"specversion":"1.0","id":"01a1ae8b-fe03-77d4-a556-738f5016d58e","source":"rdm://s0artk0pkmpp/zv17w9mxj8az/51w78g7kye7d","type":"node-record","subject":"5m5e1ve1pw74","time":"2026-11-06T01:49:32.547Z","datacontenttype":"application/json","dataschema":"node-record/2","data":{"node_id":"5m5e1ve1pw74","path":"mesh1.node","name":"1","type":"node","incarnation":"3ny63e207fr3","state":"deleting","endpoint_id":"4c6c4ad10aaa4e1f5e44866271da78ad4dc9056791efcbec75300aa3f77057fa","transport_addr":"127.0.0.1:35605","listeners":[],"runtime":{"deployment_id":"qf3cjvdyfsza","provider":"process","control_domain":"process:e152f7e9-9c84-4260-8679-e39839d273ff:4026531836","locator":{"process":{"pid":1505387,"start":65837354}}},"is_primary":1,"is_fabric_primary":null,"cert":{"issued_at":1793929770780,"expires_at":1794016170780,"issuer_expires_at":1796521770145,"renew_at":1793990769669},"signing_cert":null,"storage":{"persistence":{"persistence":"ephemeral"},"data_dir":"/dev/shm/rdm-lane-5/cells-1505235/i143-a_two_mesh_fabrics_primary_left_after_fabric_deleted_is_deleted_by_its_own_run_from_an_outside_call_inside_the_bound-1505277-0/5m5e1ve1pw74"},"executable":{"launch_id":"node","executable":"/home/admin/rdm-lanes/5/target/debug/testkit-node","sha256":"97380f0c084a0362952b6fd9fcdf6b0b540df4062d49bfd58c6502b33b48baee","image":null},"config":{},"tags":[]},"crc32":"e4215bb2"}
```

`deleted`:

```json
{"len":1385,"specversion":"1.0","id":"01a1ae8b-fe24-7bd1-bd67-4d2b234a37ca","source":"rdm://s0artk0pkmpp/zv17w9mxj8az/51w78g7kye7d","type":"node-record","subject":"5m5e1ve1pw74","time":"2026-11-06T01:49:32.579Z","datacontenttype":"application/json","dataschema":"node-record/2","data":{"node_id":"5m5e1ve1pw74","path":"mesh1.node","name":"1","type":"node","incarnation":"3ny63e207fr3","state":"deleted","endpoint_id":"4c6c4ad10aaa4e1f5e44866271da78ad4dc9056791efcbec75300aa3f77057fa","transport_addr":"127.0.0.1:35605","listeners":[],"runtime":{"deployment_id":"qf3cjvdyfsza","provider":"process","control_domain":"process:e152f7e9-9c84-4260-8679-e39839d273ff:4026531836","locator":{"process":{"pid":1505387,"start":65837354}}},"is_primary":1,"is_fabric_primary":null,"cert":{"issued_at":1793929770780,"expires_at":1794016170780,"issuer_expires_at":1796521770145,"renew_at":1793990769669},"signing_cert":null,"storage":{"persistence":{"persistence":"ephemeral"},"data_dir":"/dev/shm/rdm-lane-5/cells-1505235/i143-a_two_mesh_fabrics_primary_left_after_fabric_deleted_is_deleted_by_its_own_run_from_an_outside_call_inside_the_bound-1505277-0/5m5e1ve1pw74"},"executable":{"launch_id":"node","executable":"/home/admin/rdm-lanes/5/target/debug/testkit-node","sha256":"97380f0c084a0362952b6fd9fcdf6b0b540df4062d49bfd58c6502b33b48baee","image":null},"config":{},"tags":[]},"crc32":"41d2a748"}
```

`rdm.build.step`:

```json
{"len":488,"specversion":"1.0","id":"01a1ae8b-fcdd-7cc8-b8fb-a19e908a520c","source":"rdm://s0artk0pkmpp/bd7ebr4x2mng/zd4y41k3zta5","type":"rdm.build.step","subject":"h72998zyjysr","time":"2026-11-06T01:49:32.251Z","datacontenttype":"application/json","dataschema":"event/1","traceparent":"00-55bd55356d44f87c520f93b0bbfd3212-dbbf65d237c50ce6-01","data":{"build_id":"erskwynp44gw","mesh":"mesh2","op":"node.delete:h72998zyjysr","state":"complete","step":"NodeDeleting"},"crc32":"bde91f70"}
```

## Failure

Failures and stream certainty follow the
[caller execution contract](../node-rpc-envelope.md#caller-execution-and-certainty).

## Observability contract

Trace propagation, step spans and correlation follow the
[runtime spans](../node-rpc-envelope.md#runtime-spans).
