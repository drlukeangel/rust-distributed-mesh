# Connections: node-to-node reachability, proxy routes, and direct reconnect

**Status:** ARCHITECTURE, design of record for the fleet-wide node-to-node connections subsystem.

This document owns connection facts, their durable log and indexes, the active routing projection,
proxy/carrier routes, and the per-node direct-reconnect reconciler. It is generic across gateway,
broker, compute and node-admin nodes, and is the target generic connection/reachability substrate for
Node RPC/RDM. Broker replica routing is one consumer; it does not own this subsystem.

Mesh byte movement, pooled iroh connections, dialing, stream framing and pool mechanics remain in
[`data-plane-transport.md`](/home/admin/rafka-v2/docs/architecture/data-plane-transport.md). Topic semantic routing remains in
[`replica-routing.md`](/home/admin/rafka-v2/docs/architecture/replica-routing.md): `orgs.topics` decides WHICH broker serves an operation;
this subsystem decides HOW the source node reaches that already-chosen destination.

This ruling is explicit mesh-change permission for **#2722** within the scope defined here: the
`NodeConnection` durable-format move, Direct/Proxy connection kinds, Failed state, the latest-per-kind
indexes and virtual topics, the active connection projection, and the source-owned reconnect reconciler
in node-base. It authorizes no unrelated gossip, topology, lifecycle, forwarding, or transport change.

---

## 1. One durable connection history

There is one canonical durable connection log:

```text
<system_org>.ops.nodes.connections
```

A connection fact is node-to-node. The source and destination may be any Rafka node type. A broker
route such as `gateway -> gateway -> broker` is therefore one use of the generic model, not a special
broker table.

The raw topic remains an append-only history. Every event receives its own minted row id. The raw
history answers what happened; compacted declared indexes answer the latest state required by routing
and reconnect.

There is no separate `connections.proxies`, `connections.attempts`, `connections.dials`, or reconnect
job topic. Direct connection state, proxy connection state, and failed direct attempts are all
connection facts in this one log.

### 1.1 Connection kind

`NodeConnection` gains a durable kind:

```text
Direct
Proxy
```

`Direct` means an open data-plane connection the source originated to the destination for traffic.

Two planes, two words (R-CR5):

- **The data plane is the connections log.** Every entry is a connection a node originated for
  traffic, Direct or Proxy, with its own minted id. It is what `node.connections.get` lists, what
  `node.connections.delete { All | [ids] }` closes by name, and what a rename waits on. In Rafka the
  application originates them (a gateway opening its path to a broker); in RDM standalone the
  testkit's originate door (op `0x73`) does.
- **The control plane is gossip and the node's control-plane Node RPC pool, and neither is a connection.** Gossip neighbours are
  the overlay links the gossip framework keeps per topic and repairs itself; the node's control-plane
  Node RPC pool is the runtime's cache of dials to exact births. Nothing writes them to this log, nothing addresses them
  by id, and deleting connections never touches them: a node whose every connection is deleted is
  still heard on gossip and still answers calls. Only the node's own stop step cuts its control
  plane, after its connections are gone.

Two words for a call through a middle node, one per plane:

- **Forward** is the control plane's: a node hands a command on to the node that must execute
  it, keeps the stream for the answer, and never commands the final node itself. A peer carries
  one call verbatim to a node the caller cannot reach (the Node RPC Forward op, 0x1A, answered
  `Forwarded { inner }`); the fabric-primary hands a mesh its attempt of a Build
  (`build.attempt.run`, 0x20) and the mesh-primary executes it on the mesh's nodes (commands go
  down). Nothing is kept by the forwarder: no connection, no id, no row. A mesh-primary healing
  its own mesh from the topology it holds (R-BI7) forwards nothing; it is its own attempt.
- **Proxy** is the data plane's. A node that cannot open a direct path for traffic proves a
  two-hop path through one exact carrier and keeps it as a connection of its own: the `Proxy`
  kind below, with its minted id, listed, deleted by name, retired by this subsystem. The
  carrier carries traffic for as long as the connection stands.

The same carrier can be both at once: forwarding one command and carrying one proxy connection
are two unrelated facts, and deleting the connection never affects a forward.

`Proxy` means the source has proven a usable two-hop path to the destination through one exact carrier:

```text
source -> carrier -> destination
```

It is still logically a connection from `source` to `destination`; `carrier` records how it is realized.

### 1.2 Connection state

The durable state is:

```text
Connected
Disconnected
Failed
Refused
```

Virtual-topic classification maps those values to the architectural vocabulary:

```text
Connected    -> state=active
Disconnected -> state=dead
Failed       -> state=failed
Refused      -> state=refused
```

`Failed` is valid for Direct recovery attempts. It means only that this source failed to establish the
named direct connection. It is never global proof that the destination node is dead.

`Refused` is written by an acceptor for an inbound connection it dropped at admission
([fabric-certs.md](fabric-certs.md#on-a-connection), [node.connections.admit](lifecycles/node-connections-admit.md)),
naming the reason (`no-credential`, `bad-signature`, `expired`, `issuer-mesh-mismatch`). It is the
acceptor's fact about one dial, never proof about the dialer.

---

## 2. Target `NodeConnection` shape

Conceptually the durable row is:

```text
NodeConnection {
    id

    source
    source_type
    source_node_id
    source_incarnation_id

    destination
    destination_type
    destination_node_id
    destination_incarnation_id

    kind: Direct | Proxy
    alpn            // the registered ALPN this connection carries; a row is one transport connection on one protocol (i148)
    state: Connected | Disconnected | Failed

    // Proxy only
    carrier
    carrier_node_id
    carrier_incarnation_id

    // Direct Failed only
    recovery_epoch
    attempt_ordinal

    reason
    event_time
    logged_at
}
```

Exact Rust representation may differ, but the semantic fields and ownership above are fixed.

`source_incarnation_id`, `destination_incarnation_id`, and `carrier_incarnation_id` use
`Node.incarnation_id`, the process-birth identity landed by #2696. No second process-incarnation
identity is invented.

Adding `kind`, `Failed`, carrier fields, incarnation fields, recovery fields, or otherwise changing the
positional durable row shape is a hard durable cut. The implementing commit carries the exact marker:

```text
durable format moved
```

and follows the established full-rebirth compatibility rule.

---

## 3. The raw log and the bounded current-state indexes

The raw topic keeps every event under its own minted row id. The current-state indexes do **not** keep
every attempt as a separate member.

The index identity is:

```text
(source, destination, kind)
```

For one source/destination pair there are therefore at most two current indexed members:

```text
(source, destination, Direct) -> latest Direct record
(source, destination, Proxy)  -> latest Proxy record
```

A later event of the same kind replaces that kind's index row. The raw log still contains the older
events until normal raw-log retention removes them.

Example raw history:

```text
09:00 Direct Connected
09:12 Direct Disconnected
09:12 Direct Failed       round=R1 attempt=1
09:12 Proxy  Connected    via=gw2
09:30 Direct Failed       round=R1 attempt=2
10:30 Direct Failed       round=R1 attempt=3
14:00 Direct Connected
14:00 Proxy  Disconnected reason=direct-restored
```

During the outage the compacted current view is only:

```text
Direct -> Failed(round=R1, attempt=3, event_time=10:30)
Proxy  -> Connected(via=gw2)
```

After restoration it is only:

```text
Direct -> Connected
Proxy  -> Disconnected(reason=direct-restored)
```

Raw-log retention is therefore not part of reconnect correctness. The latest compacted Direct row carries
the recovery round, attempt ordinal and time needed to reconstruct backoff even after older raw events age
out.

### 3.1 Declared indexes

The existing by-source and by-destination rtables are extended so the member key includes `kind`.
Conceptually:

```text
by-source prefix      = hash(source)
by-destination prefix = hash(destination)
member                = hash(source, destination, alpn, kind)
```

The exact hash helper may follow existing entity-framework conventions. What is normative is one latest
member per `(source,destination,alpn,kind)`, never one member per connection-event id. Reachability is per
protocol: a working ALPN proves nothing about another, and a reconnect on one never retires another's Proxy.

### 3.2 Virtual topics

The connections kind declares virtual classifications for:

```text
state=active
state=dead
state=failed

kind=direct
kind=proxy
```

Existing node-type virtual topics remain available.

This lets consumers ask, conceptually:

```text
routing:
    rtable(source,destination).filter(state=active)

reconnect:
    latest Direct for source,destination
    where state=failed

carrier discovery:
    rtable(destination).filter(state=active).filter(kind=direct)
```

---

## 4. What is held in memory

The fleet-wide held connections projection is routing state, not a distributed retry ledger.

### 4.1 Fleet-wide Direct graph

Every routing node holds only **active Direct** connection facts fleet-wide:

```text
gw2 -> brk2 Direct Active
gw3 -> brk2 Direct Active
compute1 -> gw2 Direct Active
```

That graph is the evidence from which a source discovers possible carriers.

When the latest Direct record becomes Disconnected or Failed, the old active edge is removed from the held
graph. The Dead/Failed row itself remains durable and indexed but is not retained fleet-wide in memory.

### 4.2 Source-owned Proxy state

An active Proxy is useful only to its source. A node holds the active Proxy overrides whose source is itself.
Other nodes do not need `gw1 -> brk2 via gw2` because only gw1 can originate traffic on that proxy route.

The effective local routing view is therefore:

```text
global active Direct graph
        +
my active Proxy overrides
```

Failed and Dead Direct history is read from the durable latest-per-kind index by the owning source's
reconnect reconciler when needed. It is not gossiped into every node's resident routing graph.

---

## 5. Effective node-to-node route projection

For a source `S`, destination `D`, and protocol carrier policy, the projection answers how a **new**
invocation reaches D.

The projection reads at most:

```text
latest Direct(S,D)
latest Proxy(S,D)
```

The destination and carrier incarnations are checked against current membership; the carrier's own Direct
edge to the destination is learned from the carrier when it is asked to forward (§8).

### 5.1 Precedence

A valid active Proxy overrides an active Direct until the proxy is durably retired:

```text
if Proxy is active and valid and protocol permits its carrier:
    ViaPeer(proxy.carrier)
else if Direct is active:
    Direct
else:
    NoActiveRoute
```

This precedence provides ordered cutback. When a direct connection returns, traffic stays on the proven
Proxy until `Proxy Disconnected(reason=direct-restored)` lands. Only then does Direct become effective.

If the proxy-retirement write fails, continuing on the working proxy is safe. Node-base must retry the
retirement write until it lands.

### 5.2 Invocation snapshot

A route change affects only new invocations. An invocation that already selected and sent on a Proxy remains
on that Proxy. A sent request is never retransmitted merely because another connection later becomes
preferred.

The generic Node RPC certainty rule remains: a request may choose another route only while it is provably
`NotSent`; post-commit timeout/loss is `Indeterminate` and is never silently replayed.

---

## 6. Carrier policy and deterministic spread

The connections subsystem is generic. A protocol decides whether proxying is allowed and which node types may
carry it.

Current broker data policy is:

```text
FORWARDABLE = true
eligible carrier type = gateway
```

A non-forwardable protocol never consumes Proxy as a legal route even if one exists.

Carrier candidates are active Direct edges from an eligible carrier to the exact destination. When several
are eligible, selection is deterministic and spread by source rather than "freshest wins" herd behavior.
Conceptually:

```text
stable_hash(source, destination, destination_incarnation)
    % deterministically_ordered_eligible_carriers.len()
```

The chosen carrier is sticky until a named structural event invalidates it. A newly available carrier does
not reshuffle working routes merely by appearing.

---

## 7. Establishing a Proxy connection

Cold discovery begins only when no usable effective route is already recorded.

Example:

```text
gw1 -> brk2 Direct attempt ends NotSent
gw2 -> brk2 Direct Active
```

Sequence:

1. `gw1` writes a `Direct Failed` event for `gw1 -> brk2` with a new recovery round and
   `attempt_ordinal=1`.
2. It selects an eligible carrier from the active Direct graph.
3. It forwards the invocation once through that carrier.
4. Only a conclusive end-to-end success/refusal proving the carrier path was actually usable may establish
   the proxy route.
5. `gw1` writes `Proxy Connected`, recording the exact carrier and the destination/carrier incarnation ids.
6. Future new invocations from gw1 to brk2 use the durable Proxy override without paying the direct ladder
   again.

A post-commit answer timeout, reply loss, or other Indeterminate outcome does not prove a Proxy connection and
must not create `Proxy Connected`.

For a stream connection the conclusive answer is RDM's tunnel handshake: the destination's own `admitted`
at the exact birth, written back through the carrier on the tunnel's control stream. No application byte,
EOF or one-leg admission substitutes.

---

## 8. Proxy validity and invalidation

For:

```text
gw1 -> gw2 -> brk2
```

an active Proxy remains valid only while:

```text
destination incarnation == recorded brk2 incarnation
carrier incarnation     == recorded gw2 incarnation
gw2 -> brk2             == Direct Active
```

The two incarnations reach the source through membership. The carrier's Direct edge to the destination is
the carrier's own fact and travels nowhere: when the carrier is asked to forward and its own latest Direct to
the destination is not Active, it refuses the carried call by name (`carrier-edge-lost`, the Forward reply
`CarrierEdgeLost`), and that refusal is how the source learns the edge is gone.

If any dependency is superseded, the source writes Proxy Disconnected with a named structural reason, for
example:

```text
carrier-edge-lost
carrier-incarnation-superseded
destination-incarnation-superseded
```

The source may then select another currently eligible carrier and establish a new Proxy, or have no active
route.

A reply timeout after request commit is not proxy-health evidence and does not invalidate the proxy.

---

## 9. Direct reconnect is owned by every node

Every node runs the same reconnect mechanism internally in `rafka-node-base`. Node-admin does not monitor
Proxy routes and does not send reconnect commands.

This is deliberately **not** a normal platform `Job` entity. It is a local durable reconciler whose work is
reconstructed from `ops.nodes.connections`.

Why: the source node already owns its connection observations, owns its Direct/Proxy rows, has the transport
context needed to dial, and is the only process that can repair its own source-to-destination direct edge.
Routing local transport repair through node-admin would centralize an N-by-N reachability problem and add a
control hop to state the source already knows.

Target module, exact filename may differ:

```text
crates/rafka-node-base/src/connection_reconnect.rs
```

It contains no topic, broker-replication, Kafka, org, or node-admin orchestration semantics.

### 9.1 Reconnect obligation

A reconnect series exists when the latest Direct record for this source and destination is Failed for the
current destination incarnation and no newer Direct Connected supersedes it.

An active Proxy commonly accompanies that state, but recovery does not depend on the Proxy continuing to
exist. If the carrier disappears, restoring Direct is still desirable.

### 9.2 Recovery round

A new independent direct-failure incident starts a new `recovery_epoch`.

```text
E1: Failed 1 -> Failed 2 -> Failed 3 -> Direct Connected
...
E2: Failed 1
```

E2 does not inherit E1's ordinal.

### 9.3 Durable backoff

Every failed reconnect attempt appends a new `Direct Failed` event with:

```text
recovery_epoch = E
attempt_ordinal = N + 1
event_time      = T
reason          = exact transport refusal/failure
```

That row replaces the Direct member in the compacted `(source,destination,Direct)` index.

The next due time is reconstructed deterministically from the latest indexed Failed record:

```text
next_due = event_time
         + reconnect_backoff(attempt_ordinal)
         + deterministic_jitter(source, destination, attempt_ordinal)
```

A process restart therefore does not reset exponential backoff. The node reads its latest Direct-per-
destination index rows and reconstructs only the currently relevant schedule. It never replays the raw log.

The exact constants are implementation policy and must be tested, but the schedule is bounded, exponentially
backed off, jittered, and may continue at its maximum interval while the destination incarnation remains
relevant. A restart before `next_due` must not trigger an early attempt.

### 9.4 One scheduler per node

Node-base runs one scheduler/reconciler per node process, with at most one pending direct-reconnect item per:

```text
(destination, destination_incarnation)
```

It is not one sleeping task per topic, request, or protocol. Broker topics sharing the same destination all
benefit from the same node-to-node connection state.

Two paths arm that one reconciler and that one pending item: the destination falling due under its durable
Failed rows (§9.5), and a connection accepted from the destination, which is a recovery trigger (§10, §13 G).
An accept arriving while an item is pending coalesces into it; it never creates a second attempt and never
skips the backoff the durable rows set.

### 9.5 One reconnect attempt

When a destination becomes due:

1. Verify its `Node.incarnation_id` still matches the Failed record.
2. Verify no Direct Connected already supersedes the obligation.
3. Perform one bounded direct transport connection attempt.
4. On failure, append the next Direct Failed event and reschedule from that durable row.
5. On success, normal pooled-connection machinery emits Direct Connected. The reconnect module does not
   invent a parallel success fact.

Failed attempts change no global liveness state and never ask node-admin to replace/remove the destination.

---

## 10. Any Direct Connected retires the Proxy

Direct recovery is event-driven at truth transition, regardless of why the direct pooled connection opened.

A Direct Connected may come from:

```text
the reconnect reconciler's dial
normal transport activity
another permitted direct-connection establishment path
```

All are equivalent evidence.

Whenever node-base observes:

```text
Direct(S,D) == Connected
Proxy(S,D)  == Connected
```

it owes:

```text
Proxy Disconnected(reason=direct-restored)
```

The proxy-retirement write is itself reconciled. If it fails, keep routing over Proxy and retry the write.
After restart, the same pair of latest indexed rows reconstructs the pending retirement automatically.

This gives ordered restoration:

```text
Direct Connected appears
        |
        v
Proxy still effective
        |
        v
Proxy Disconnected durably lands
        |
        v
Direct becomes effective for new invocations
```

---

## 11. Relationship to semantic routing and #2640

This subsystem never decides which broker should serve a topic operation.

For broker traffic:

```text
Topic + orgs.topics
    -> WHICH broker serves / which write leg is suppressed

connections
    -> HOW this gateway reaches the chosen broker
```

A Direct/Proxy transition does not create or advance an `orgs.topics` route generation, does not satisfy or
invalidate a route-barrier participant, and requires no #2640 ACK.

Conversely, an `orgs.topics` route change does not rewrite connection rows merely because a different broker
is now semantically selected.

---

## 12. Observability

Every durable connection transition retains its existing connection update evidence and names at least:

```text
source
destination
kind
state
reason
```

Proxy transitions additionally identify the carrier. Reconnect attempts identify the recovery round and
attempt ordinal. An admission verdict is spanned `rdm.node.connections.create.via-admitted` or
`rdm.node.connections.reject.via-refused` with the named reason, on the acceptor.

The reconnect attempt span is owned by node-base and records whether it:

```text
connected
failed
was superseded before dial
was already satisfied by Direct Connected
```

A successful pooled connection remains the authoritative Direct Connected event. The reconnect span is not a
second source of connection truth.

Cold discovery continues to expose `effective_route_cold_discovery_ms` so the first-use cost after a genuinely
unknown destination is visible.

---

## 13. Required acceptance tests

### A. One discovered proxy serves every consumer of the same destination

Topology:

```text
gw1 !-> brk2
gw2  -> brk2
```

First operation from gw1 proves and writes `Proxy Connected via gw2`. A second operation, including one for a
different topic that selects the same brk2, must use the Proxy immediately. Forbidden: another full direct
dial ladder before forwarding.

### B. Proxy survives source restart

With latest indexed rows:

```text
Direct -> Failed(E1,N)
Proxy  -> Connected(via=gw2)
```

restart gw1. The first operation after hydration uses the Proxy without rediscovering it.

### C. Failed reconnect backoff survives restart

Write `Direct Failed(E1, attempt=N, event_time=T)` such that `next_due` is in the future, restart the source,
and assert no attempt occurs before the reconstructed due time. Forbidden: restart at attempt 1.

### D. Index stays bounded

Generate many failed reconnect attempts for one pair. Assert the raw log contains the history while the
current indexes contain at most one Direct member and one Proxy member for that source/destination pair.

### E. Failed/Dead records do not pollute fleet-wide held routing

Generate repeated reconnect failures on gw1. Assert peer nodes' resident connection projection contains only
active Direct routing edges, never gw1's failed/dead retry history.

### F. Reconnect success returns to Direct in order

Start with active Proxy and failed Direct. Restore direct connectivity. Assert:

```text
Direct Connected written
Proxy remains effective
Proxy Disconnected(reason=direct-restored) written
Direct becomes effective
```

### G. An inbound accept is a recovery trigger

With Proxy active, make the destination dial the source first. Assert an accepted inbound connection from the
destination is a recovery trigger: the source attempts its own direct connection, and only its Direct Connected
creates the retirement obligation.

### H. Proxy retirement write failure is safe and retried

Force the Proxy Disconnected durable write to fail after Direct Connected. Assert traffic stays on Proxy,
node-base keeps a retirement obligation, and a later successful durable write moves new traffic to Direct.

### I. Destination/carrier incarnation fencing

A destination incarnation change invalidates the old Direct/Proxy evidence for that process birth. A carrier
incarnation change invalidates a Proxy that names the old carrier process. Stale evidence is never used for a
new process birth.

### J. Carrier edge loss invalidates Proxy

Drop the carrier's active Direct edge to destination. The carrier refuses the next carried call with
`carrier-edge-lost`; the source writes Proxy Disconnected with that reason, stops selecting that Proxy, and
either establishes another valid Proxy or exposes no active route.

### K. Sent request is never rerouted

Force a post-commit answer timeout. Assert the request is not resent through another connection and the timeout
alone does not invalidate a working Proxy.

### L. #2640 independence

Switch effective reachability Direct -> Proxy -> Direct without changing the semantic destination. Assert no
`orgs.topics` generation, route-barrier ACK, or watermark transition is created by those connection changes.

---

## 14. Ownership and RDM target

Current Rafka implementation homes:

```text
crates/rafka-mesh-ops/src/connections.rs
    durable shape, declared indexes/VTs, connection projection contracts

crates/rafka-node-base/src/connection_rows.rs
    source-owned connection event writes

crates/rafka-node-base/src/connection_reconnect.rs
    per-node reconnect and proxy-retirement reconciliation

crates/rafka-reach-reroute/
    downstream consumer/adapter during migration
```

The generic model ultimately moves upstream with the RDM mesh/entity/transport/Node-RPC substrate. RDM owns the
generic Direct/Proxy connection model, incarnation-bound reachability, reconnect reconciliation, and protocol
carrier-policy hook. Rafka owns Rafka protocol descriptors and product-specific carrier policy such as
`broker_data: gateway carriers only`.

---

## 15. Invariants

1. `ops.nodes.connections` is the one durable history of node-to-node connection facts.
2. The subsystem is generic across every node type; broker routing is only one consumer.
3. Direct and Proxy are first-class connection kinds in `NodeConnection`.
4. Connected/Disconnected/Failed map to active/dead/failed virtual classifications.
5. The raw log keeps event history; current indexes keep one latest Direct and one latest Proxy per pair.
6. The current index never grows one member per retry attempt.
7. Fleet-wide resident connection state contains active Direct facts only.
8. Active Proxy state is source-owned and consumed by that source's effective route projection.
9. Failed and Dead retry history is durable/indexed and read on demand, not held fleet-wide.
10. A valid active Proxy overrides Direct until the Proxy is durably retired.
11. Every node owns repair of its own direct connections in node-base.
12. Node-admin does not schedule or command local direct reconnect attempts.
13. Reconnect is an internal durable reconciler, not a normal platform Job entity.
14. The latest Direct Failed row is sufficient to reconstruct retry round, ordinal and next due time.
15. Restart never resets reconnect exponential backoff.
16. Any Direct Connected event, including an accepted inbound connection, creates the proxy-retirement obligation.
17. Failed proxy-retirement writes are retried; routing safely remains on Proxy until retirement lands.
18. Destination and carrier process incarnations make stale route evidence refusable by name.
19. A failed direct attempt is source-local reachability evidence, not global node-death proof.
20. Customer traffic does not periodically probe Direct while a durable Proxy is active.
21. A sent/committed invocation is never silently rerouted or replayed.
22. Connection routing is independent of `orgs.topics` semantic-route convergence and #2640.
23. The `NodeConnection` row change carries `durable format moved` and requires the established full rebirth.
24. #2722 is authorized to make exactly the mesh/transport changes required by this design.
