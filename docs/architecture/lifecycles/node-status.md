# Node status

[Lifecycle index](../fabric-mesh-node-lifecycles.md) · [States](states.md) · [Typed calls and wire mapping](../node-rpc-envelope.md#typed-object-contract)

The internal status report and apply; not a caller op. Status moves only through the
lifecycle actions and workflows, never a meta or config put
([Meta and config](../node-rpc-envelope.md#node-properties)). The
[Status contract](../node-rpc-envelope.md#status-op-0x1b-rdm) owns the codec, the direction
and transition checks and the refusal fields.

<a id="node-status-declaration"></a>

## Status report

A report is sent when owed and again only on an eligible event:

- its primary's address changes to a deliverable exact instance;
- a failed delivery gains a viable route;
- the primary addresses this node with the state's down call.

Typed replies, including named refusals, are terminal. One coalesced wake drives the
work, never a timer. An unreachable target schedules no report. The exact-instance read is
[node.tickle](node-tickle.md)'s probe, `ProbeNodeState`; its Current reply reasserts held
status and does not manufacture a transition or a new change timestamp.

A node reports to its mesh's primary, which sets its status. A report is an owed message
([Node RPC §43](../node-rpc.md#43-owed-messages-delivery-is-the-runtimes)) and names the operation that
produced the state when one did ([DeclaredOperation](../node-rpc-envelope.md#status-op-0x1b-rdm)): a stop's
`Stopped`, from which the mesh-primary publishes NodeStopped ([node.stop](node-stop.md) step 5), and a
start's `Pending` and `Ready`.

<a id="node-status-change"></a>

## Status apply

| Field | Contract |
|---|---|
| Caller / receiver | The node's mesh-primary, downward to the exact accepted node |
| Kept | node.id, incarnation, endpoint key, port, immutable path and runtime |
| Mapping | ApplyNodeState on the [Status schema](../node-rpc-envelope.md#status-op-0x1b-rdm); connections-deleting is entered only by [node.connections.delete](node-connections-delete.md) with `{ scope: All }` |
| Result | a typed reply, or a named typed refusal |
| Completion | Stream result for the accepted exact subject; no runtime exit inferred |

Workflow actions run the status apply; how a node leaves the stopped status is
[Stopped nodes](states.md#stopped-nodes).

On the applied status the node adjusts its lifecycle admission. No provider stop,
endpoint bind, new JoinNode, identity release or replacement is implied.

A refusal is typed and named; the workflow that ran the apply records it as its own
step's `Failed { step, reason }`.

## Observability contract

Trace propagation, step spans and correlation follow the
[runtime spans](../node-rpc-envelope.md#runtime-spans).
