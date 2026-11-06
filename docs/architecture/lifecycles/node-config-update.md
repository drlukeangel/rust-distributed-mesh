# Node config update

[Lifecycle index](../fabric-mesh-node-lifecycles.md) · [States](states.md) · [Typed calls and wire mapping](../node-rpc-envelope.md#typed-object-contract)

`node.config.update` replaces the whole application config map: a PUT. RDM stores the values
without interpreting them. A node owns its own part of its record and persists changes through its one
serialized property writer. The node's properties and its status and role flags are in the node's one record (Q323), which the node writes (Q356); node-side application mutates only properties and moves no primary role. Deferred acceptance
records its identified obligation separately in lifecycle state.

## Numbered end-to-end sequence

### A caller's update

1. The caller sends the command to the fabric-primary, which forwards it through the node's
   mesh-primary to the exact node birth: `UpdateConfig { node_id, incarnation, config }` on
   [`0x22`](../node-rpc-envelope.md#nodecommand-op-0x22-rdm) at every hop, each one direct call; an
   unreachable receiver ends the call `NotSent`. The node's `CommandAccepted` is
   `node.config.updating`, and each hop streams the frames back. The stream ends
   `PropertyUpdated { reply }` with `ConfigUpdated`, `node.config.updated`.
2. The node's property writer applies the whole map to its latest owned state and issues one put.
   The writer retains serialization through terminal persistence and local publication.
3. Completion follows durable persistence. The completion carries no node-record revision and
   causes no copy of the map to be installed in either primary's atlas.
4. A reader that needs config reads the node's own part of its record at its node ID. A read failure is
   reported by name; a stale copy is not substituted.

### A node's own update

1. The node calls its local property writer with the whole map.
2. The writer persists it and publishes the durable result locally before answering.
3. No command or map travels upward to either primary. The ordinary completion and persistence
   spans retain the operation's causal context.

The same writer serializes local changes, caller commands, tags, Build entry application and
bound-address publication. Config and tags are independent fields; changing config does not
replace tags. A map is not logged in a span or error message.

## Build application

A Build applies its entry to new and existing nodes. An existing node's `ApplyNodeProperties`
command persists config and tags in
one durable put. The existing target receipt fixes
the exact birth; workflow receipts track completion of the operation and its metadata changes.

A later Build has a new ID and
can override node or caller changes. True recovery inherits the predecessor's acknowledged properties.
A Create initializes from its own Build entry, including an empty map.

## Deferred application and startup

A stopped process remains alive with its Node RPC endpoint bound and applies an update as a running
node does. A node that does not answer ends the command `ConfigNotApplied` by name; nothing is
recorded for it.

Launch properties initialize an absent own record. Existing durable properties survive startup; an older environment must not overwrite them. The own-property
writer may read its retained state. Startup operations supply the lifecycle/topology context that
an ordinary node needs, without requiring it to reconstruct another node's record.

## States written

This operation writes no state. The node writes the map on its own record (Q356), as `config` on a `node-record` line in `nodes.jsonl`.

| written | by | when |
|---|---|---|
| `config` | the node, on its own record | it accepted the command, before `node.config.updated` |

As stored:

`config`:

```json
{"len":1485,"specversion":"1.0","id":"01a1ae8c-09ec-7853-b23f-6c9eace14f64","source":"rdm://9ct8f4h6006k/9demt04km0bq/10ftfnacwtxf","type":"node-record","subject":"9demt04km0bq","time":"2026-11-06T01:49:35.595Z","datacontenttype":"application/json","dataschema":"node-record/2","data":{"node_id":"9demt04km0bq","path":"mesh1.node-admin","name":"1","type":"node-admin","incarnation":"10ftfnacwtxf","state":"ready","endpoint_id":"5bf719be9591c1288f32db4587d8a014985fb7b870854aff94f3c128e4814e1a","transport_addr":"127.0.0.1:35013","listeners":[],"runtime":{"deployment_id":"4drjw71rchk3","provider":"process","control_domain":"process:e152f7e9-9c84-4260-8679-e39839d273ff:4026531836","locator":{"process":{"pid":1506281,"start":65837752}}},"is_primary":1,"is_fabric_primary":1,"cert":{"issued_at":1793929774901,"expires_at":1794016174901,"issuer_expires_at":1796521774901,"renew_at":1793989964798},"signing_cert":{"issued_at":1793929774901,"expires_at":1796521774901,"renew_at":1795703414510},"storage":{"persistence":{"persistence":"persistent","disposition":"preserve"},"data_dir":"/dev/shm/rdm-lane-5/cells-1506210/i143-a_lone_fabric_primary_keeps_its_own_config_across_its_restart-1506260-0/mesh1.node-admin.1"},"executable":{"launch_id":"node-admin","executable":"/home/admin/rdm-lanes/5/target/debug/rdm-node-admin","sha256":"8cb2122a53f91157b0e89b0cca40fbcb00822a487e44b9a71dc53cc14834ce5f","image":null},"config":{"listener.data":"127.0.0.1:45007"},"tags":[]},"crc32":"e99e932d"}
```

## Failure

Failures and stream certainty follow the
[caller execution contract](../node-rpc-envelope.md#caller-execution-and-certainty).
A put's returned error may conceal a committed effect whose acknowledgement was lost. Every return
is terminal for that issued mutation: it cannot commit later. Cancellation retains the original
write's lifetime until terminal completion; a transport deadline is not absence and is not a retry.
Resolve the exact durable application before repeating an uncertain Build effect.

## Observability contract

Trace propagation, step spans and correlation follow the
[runtime spans](../node-rpc-envelope.md#runtime-spans). Spans identify the exact subject, application
identity, backend and outcome. They never include config values, credentials or launch secrets.
