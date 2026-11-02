# Node config update

[Lifecycle index](../fabric-mesh-node-lifecycles.md) · [States](states.md) · [Typed calls and wire mapping](../node-rpc-envelope.md#typed-object-contract)

`node.config.update` replaces the whole application config map: a PUT. RDM stores the values
without interpreting them. A node owns its property record and persists changes through its one
serialized property writer. The node's properties and the status and role flags its mesh-primary writes are in the node's one record (Q323); node-side application mutates only properties and moves no primary role. Deferred acceptance
records its identified obligation separately in lifecycle state.

## Numbered end-to-end sequence

### A caller's update

1. The caller sends the command to the fabric-primary, which forwards it through the node's
   mesh-primary to the exact node birth. Commands keep the existing downward route.
2. The node's property writer applies the whole map to its latest owned state and issues one put.
   The writer retains serialization through terminal persistence and local publication.
3. Completion follows durable persistence. The completion carries no node-record revision and
   causes no copy of the map to be installed in either primary's atlas.
4. A reader that needs config reads the node's property record at its node ID. A read failure is
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
Replacement initializes from its own Build entry, including an empty map.

## Deferred application and startup

A stopped process remains alive with its Node RPC endpoint bound and applies an update as a running
node does. A node that does not answer ends the command `ConfigNotApplied` by name; nothing is
recorded for it.

Launch properties initialize an absent own record. Existing durable properties survive startup; an older environment must not overwrite them. The own-property
writer may read its retained state. Startup operations supply the lifecycle/topology context that
an ordinary node needs, without requiring it to reconstruct another node's lifecycle records.

## Failure and receipts

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
