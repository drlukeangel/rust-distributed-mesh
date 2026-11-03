# Connections: node-to-node reachability, proxy routes, and direct reconnect

**Status:** ARCHITECTURE, design of record for the fleet-wide node-to-node connections subsystem.

This document owns connection facts, their durable log and indexes, the active routing projection,
proxy/carrier routes, and how a source returns to Direct. It is generic across gateway,
broker, compute and node-admin nodes, and is the target generic connection/reachability substrate for
Node RPC/RDM. Broker replica routing is one consumer; it does not own this subsystem.

Mesh byte movement, pooled iroh connections, dialing, stream framing and pool mechanics are the
transport's, not this subsystem's. Which node serves an application operation is the application's
routing; this subsystem decides HOW the source node reaches that already-chosen destination.

This ruling is explicit mesh-change permission for **#2722** within the scope defined here: the
`NodeConnection` durable-format move, Direct/Proxy connection kinds, Failed state, the latest-per-kind
indexes and virtual topics, the active connection projection, and the source's return to Direct
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
history answers what happened; compacted declared indexes answer the latest state per pair, which the
mesh-primary's node investigation reads when it runs. No node reads its rows back to route or reconnect
(§4.1).

There is no separate `connections.proxies`, `connections.attempts`, `connections.dials`, or reconnect
job topic. Direct connection state, proxy connection state, and failed direct attempts are all
connection facts in this one log.

Every connection write is asynchronous: it is owed on the node's one ordered writer queue, retried
with backoff until it lands; no open, dial, accept or read waits for it; the held projection takes a
fact once its write lands. Q169.

### 1.1 Connection kind

`NodeConnection` gains a durable kind:

```text
Direct
Proxy
```

`Direct` means an open data-plane connection the source originated to the destination for traffic.

Two planes, two words (R-CR5):

- **The data plane is the connections log.** Every entry is a connection a node originated for
  traffic, Direct or Proxy, with its own minted id. It is what `node.connection.get` lists, what
  `node.connection.delete { All | [ids] }` closes by name, and what a rename waits on. In Rafka the
  application originates them (a gateway opening its path to a broker); in RDM standalone the
  testkit's originate service (op `0x73`) does.
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
  `Forwarded { inner }`); the fabric-primary hands a mesh its section of a Build
  (`build.section.run`, 0x20) and the mesh-primary executes it on the mesh's nodes (commands go
  down). Nothing is kept by the forwarder: no connection, no id, no row. A mesh-primary healing
  its own mesh from the topology it holds (R-BI7) forwards nothing; it is its own workflow.
- **Proxy** is the data plane's. A node that cannot open a direct path for traffic shows a
  two-hop path through one exact carrier and keeps it as a connection of its own: the `Proxy`
  kind below, with its minted id, listed, deleted by name, deleted by this subsystem. The
  carrier carries traffic for as long as the connection stands.

The same carrier can be both at once: forwarding one command and carrying one proxy connection
are two unrelated facts, and deleting the connection never affects a forward.

`Proxy` means the source has shown a usable two-hop path to the destination through one exact carrier:

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

`Failed` is valid for a Direct dial whose `Send` attempts are spent (§9.3). It means only that this source failed to establish the
named direct connection. It is never global evidence that the destination node is dead.

`Refused` is written by an acceptor for an inbound connection it dropped at admission
([fabric-certs.md](fabric-certs.md#on-a-connection), [node.connection.create](lifecycles/node-connections-admit.md)),
naming the reason, one of node.connection.create's `refused` reasons (its Result row). It is the
acceptor's fact about one dial, never evidence about the dialer.

### 1.3 One dial: its rows and its spans

A connection is created by its caller, and only by it: the dial is node.connection.create, and the
member cert rides it in the first stream's present frame, never a separate call. The receiver serves or
refuses that presentation and creates nothing. Each side writes only its own facts; each recorded fact is
one whole append-only row (§3); an accept writes nothing (R-CN1).

| Step | The caller | The receiver |
|---|---|---|
| Dial | `rdm.node.connection.create.via-dial` opens (destination, alpn, cert_expires_at), a root linked to the opener's span; the present frame carries the opener's trace (or the dial's when the opener carries none) | — |
| Admitted | the dial ends `outcome = connected`; it writes `Direct Connected` | `rdm.node.connection.serve.via-admitted` (source, destination, connection, alpn, cert_expires_at, clock), in the caller's trace; no row |
| Refused | the dial ends `outcome = refused` with the `reason`; it writes `Direct Failed`, reason `refused: <reason>` (§9.3) | `rdm.node.connection.reject.via-refused` (the same, plus `reason`); it writes `Refused` naming the reason |
| Through a carrier | The opener's `rdm.node.connection.create.via-tunnel` carries `cert_expires_at`; the carrier dials the destination. After the destination's `admitted` comes back through the carrier (§7), the opener writes `Proxy Connected` naming the carrier. | the destination serves or refuses as above |
| Ended | `Disconnected` when the connection closes or is deleted (node.connection.delete); a tunnel's end is spanned `rdm.node.connection.delete.via-tunnel-ended` and writes no second row | — |

`Refused` and `Failed` are each one side's fact about one dial: neither is evidence about the other side.

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
    state: Connected | Disconnected | Failed | Refused

    // Proxy only
    carrier
    carrier_node_id
    carrier_incarnation_id

    reason
    event_time
    logged_at
}
```

Exact Rust representation may differ, but the semantic fields and ownership above are fixed.

`source_incarnation_id`, `destination_incarnation_id`, and `carrier_incarnation_id` use
`Node.incarnation_id`, the process-birth identity landed by #2696. No second process-incarnation
identity is invented.

Adding `kind`, `Failed`, carrier fields, incarnation fields, or otherwise changing the
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
(source, destination, alpn, kind)
```

For one source/destination/alpn triple there are therefore at most two current indexed members:

```text
(source, destination, alpn, Direct) -> latest Direct record
(source, destination, alpn, Proxy)  -> latest Proxy record
```

A later event of the same kind replaces that kind's index row. The raw log still contains the older
events until normal raw-log retention removes them. In RDM's file backend the raw log and the current
view are one file, `connections.jsonl`, the view the fold's latest line per `subject`.

Example raw history:

```text
09:00 Direct Connected
09:12 Direct Disconnected
09:12 Direct Failed
09:12 Proxy  Connected    via=gw2
09:30 Direct Failed
10:30 Direct Failed
14:00 Direct Connected
14:00 Proxy  Disconnected reason=direct-restored
```

During the outage the compacted current view is only:

```text
Direct -> Failed(event_time=10:30)
Proxy  -> Connected(via=gw2)
```

After restoration it is only:

```text
Direct -> Connected
Proxy  -> Disconnected(reason=direct-restored)
```

Raw-log retention is therefore not part of reconnect correctness: no dial reads a row (§9).

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
protocol: a working ALPN shows nothing about another, and a reconnect on one never deletes another's Proxy.

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

This lets the mesh-primary's node investigation ask, when it runs ([node investigation](lifecycles/node-investigation.md)),
conceptually:

```text
leads:
    rtable(destination).filter(state=active).filter(kind=direct)
```

Routing, reconnect and the carrier pick read no row: a node holds its own facts from the moment each
write lands (§4.1). A read of other nodes' facts ahead of a Proxy or an investigation is stale the moment
it is read (Q227).

---

## 4. What is held in memory

Each node's held connections are routing state, not a distributed retry ledger.

### 4.1 A node holds its own facts

A node holds the connection facts it writes, each from the moment its write lands: its latest Direct per
`(destination, alpn)` and its own active Proxy per `(destination, alpn)`. It holds no other node's facts,
and it reads none from the store on create, start or rejoin, or before a carrier pick (Q227). A node is
born holding no connection: a restart closes every connection the node held.

When the latest Direct record becomes Disconnected or Failed, the old active connection is removed from what the
node holds. The Dead/Failed row itself remains durable and indexed.

### 4.2 Source-owned Proxy state

An active Proxy is useful only to its source. A node holds the active Proxy overrides whose source is itself.
Other nodes do not need `gw1 -> brk2 via gw2` because only gw1 can originate traffic on that proxy route.

The effective local routing view is therefore:

```text
my latest Direct facts
        +
my active Proxy overrides
```

The owning source holds its latest Failed Direct itself, from its own write (§9.3). No node
reads it back from the store, and it is not gossiped to any other node.

---

## 5. Effective node-to-node route projection

For a source `S`, destination `D`, and protocol carrier policy, the projection answers how a **new**
invocation reaches D.

The projection reads at most:

```text
latest Direct(S,D)
latest Proxy(S,D)
```

The destination and carrier incarnations are checked against the current topology view; the carrier's own Direct
connection to the destination is learned from the carrier when it is asked to carry (§8).

### 5.1 Precedence

A valid active Proxy overrides an active Direct until the proxy is durably deleted:

```text
if Proxy is active and valid and protocol permits its carrier:
    ViaPeer(proxy.carrier)
else if Direct is active:
    Direct
else if the latest Direct is Failed:
    NoActiveRoute
else:
    Direct
```

A latest Failed Direct records the last dial whose `Send` was spent; nothing is scheduled from it (§9).
Any other non-active Direct is none ever observed, a closed connection (Disconnected, as
`node.connection.delete` writes at every node.stop), or an acceptor's Refused. A new call
dials it (§7), and a dial whose attempts are spent writes its Failed (§9.3).

This precedence provides ordered cutback. When a direct connection returns, traffic stays on the working
Proxy until `Proxy Disconnected(reason=direct-restored)` lands. Only then does Direct become effective.

If the proxy-delete write fails, continuing on the working proxy is safe. Node-base must retry the
delete write until it lands.

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

A protocol's policy is the application's (§14); Rafka's broker-data policy, proxied with gateway
carriers only, is stated on Rafka's pages.

A protocol that does not proxy never consumes Proxy as a legal route even if one exists.

Carrier candidates are the live nodes of the source's mesh, from the topology it holds, of a node type the
protocol lets carry, the source and the destination left out. The source asks them in path order,
deterministic and spread by source rather than "freshest wins" herd behavior, starting at:

```text
stable_hash(source, destination, destination_incarnation)
    % deterministically_ordered_candidates.len()
```

and wrapping. The carrier's own Direct to the destination decides: a candidate without an Active one
refuses by name (`connection-lost`, §8) and the source asks the next; when every candidate refuses, the
open ends with no active route. No connection fact is read to choose.

The chosen carrier stays chosen until a named structural event invalidates it. A newly available carrier does
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

1. `gw1` writes a `Direct Failed` event for `gw1 -> brk2` naming the dial's reason.
2. It asks the candidates of §6 in their order; the first whose own Direct to the destination is Active
   answers.
3. It proxies the invocation once through that carrier.
4. Only a conclusive end-to-end success/refusal showing the carrier path was actually usable may establish
   the proxy route.
5. `gw1` writes `Proxy Connected`, recording the exact carrier and the destination/carrier incarnation ids.
6. Future new invocations from gw1 to brk2 use the durable Proxy override without paying the direct ladder
   again.

A post-commit answer timeout, reply loss, or other Indeterminate outcome does not show a Proxy connection and
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

The two incarnations reach the source through the topology view. The carrier's own Direct to the destination is
the carrier's own fact and travels nowhere: when the carrier is asked to carry and its own latest Direct to
the destination is not Active, it refuses by name (`connection-lost`: on a Proxy connection the tunnel's
refusal on its control stream, on a Node RPC call carried over the Proxy the Forward reply
`ConnectionLost`), and that refusal is how the source learns the carrier's Direct is gone.

If any dependency is superseded, the source writes Proxy Disconnected with a named structural reason, for
example:

```text
connection-lost
carrier-incarnation-superseded
destination-incarnation-superseded
```

The source may then select another currently eligible carrier and establish a new Proxy, or have no active
route.

A reply timeout after request commit is not proxy-health evidence and does not invalidate the proxy.

---

## 9. Direct reconnect is owned by every node

Every node restores its own Direct connections in `rdm-node`. Node-admin does not monitor Proxy routes
and sends no reconnect commands. No connection retries on its own: every dial is a call on `Send`
([Node RPC §43](node-rpc.md#43-send-delivery-is-the-runtimes); Q344, Q348), and nothing schedules a
dial from a stored row.

Why: the source node already owns its connection observations, owns its Direct/Proxy rows, has the
transport context needed to dial, and is the only process that can repair its own source-to-destination
connection.

```text
crates/rdm-node/src/connection_reconnect.rs
```

It contains no topic, broker-replication, Kafka, org, or node-admin orchestration semantics.

### 9.1 A lost Direct

When a Direct connection is lost, the application's next open dials Direct on `Send`. If no Direct is
made within its attempts, the open's carriers answer (§6, §7) and a Proxy may be shown. When no route is
found and the attempts are spent, the dial ends to the application by name and `on_call_failed` is
called ([fabric-node-hooks.md](fabric-node-hooks.md#on_call_failed)); the application opens again if it
wants.

### 9.2 Back to Direct

While a Proxy is held, an open uses it (§5, §7 step 6). The source returns to Direct when:

- **the destination dials the source.** A connection accepted from the destination's exact birth while a
  Proxy to it is active is the trigger (§10, §13 G): the source makes one Direct dial on `Send`, one per
  `(destination, destination_incarnation, alpn)` and one at a time on the node; an accept arriving
  while that dial runs joins it and never starts a second, a dial toward a birth the view no longer
  holds is dropped undialled, and a dial whose attempts are spent ends: only the next accept starts
  another;
- **the Proxy is invalidated** (§8): the next open dials Direct first.

### 9.3 One dial

1. Verify the destination's `Node.incarnation_id` is the one the dial names.
2. Verify no Direct Connected already holds for it.
3. Dial on `Send`: a dial that sent nothing is made again within its attempts.
4. A dial whose attempts are spent writes one `Direct Failed` row, from its last attempt, with the
   transport's reason: one dial is one row.
5. On success, normal pooled-connection machinery emits Direct Connected. The reconnect module does not
   invent a parallel success fact.

A failed dial changes no global liveness state and never asks node-admin to remove the destination. A
restart closes every connection, and the restarted node reads no row back (Q227).

---

## 10. Any Direct Connected deletes the Proxy

Direct recovery is event-driven at truth transition, regardless of why the direct pooled connection opened.

A Direct Connected may come from:

```text
the accept-triggered Direct dial (§9.2)
normal transport activity
another permitted direct-connection establishment path
```

All are equivalent evidence.

Whenever node-base observes, for one ALPN:

```text
Direct(S,D,alpn) == Connected
Proxy(S,D,alpn)  == Connected
```

it owes:

```text
Proxy Disconnected(reason=direct-restored)
```

The proxy-delete write is itself reconciled. If it fails, keep routing over Proxy and retry the write.
A restarted node owes none: it holds no Proxy.

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

A stream Proxy (i148, a tunnel) outlives its delete. `Proxy Disconnected(reason=direct-restored)` deletes
the route, so the next `open` goes Direct; the tunnel that is already open and its streams run to their own
end. Until then it is a held connection (§4.2): `node.connection.get` lists it by its row id with
`route: deleted`, and `node.connection.delete { All | [ids] }` closes it by that id like any other. Its end
writes no second row; it is spanned `rdm.node.connection.delete.via-tunnel-ended`.

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

Proxy transitions additionally identify the carrier. An admission verdict is spanned `rdm.node.connection.serve.via-admitted` or
`rdm.node.connection.reject.via-refused` with the named reason, on the acceptor.

The accept-triggered Direct dial's span (§9.2) is owned by node-base and records whether it:

```text
connected
failed
was superseded before dial
was already satisfied by Direct Connected
```

A successful pooled connection remains the Direct Connected event. The reconnect span is not a
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

First operation from gw1 shows and writes `Proxy Connected via gw2`. A second operation, including one for a
different topic that selects the same brk2, must use the Proxy immediately. Forbidden: another full direct
dial ladder before proxying.

### B. A restarted source holds no Proxy

With latest indexed rows:

```text
Direct -> Failed
Proxy  -> Connected(via=gw2)
```

restart gw1. The restarted gw1 holds no Direct and no Proxy and reads none back; its first operation dials
Direct and, on NotSent, shows its carrier again (§7). Forbidden: a route over a Proxy the restarted birth
did not show.

### C. A restart holds no Failed Direct

Write `Direct Failed`, restart the source, and assert the restarted source holds no Failed Direct and
its next dial toward the destination is a fresh call on `Send`.
Forbidden: the restarted source reading the row back.

### D. Index stays bounded

Generate many failed dials for one pair. Assert the raw log contains the history while the
current indexes contain at most one Direct member and one Proxy member for that source/destination pair.

### E. Failed/Dead records stay with their source

Generate repeated reconnect failures on gw1. Assert no peer node holds any of gw1's facts: a node holds its
own facts only (§4.1).

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
creates the delete obligation.

### H. Proxy delete write failure is safe and retried

Force the Proxy Disconnected durable write to fail after Direct Connected. Assert traffic stays on Proxy,
node-base keeps a delete obligation, and a later successful durable write moves new traffic to Direct.

### I. Destination/carrier incarnation fencing

A destination incarnation change invalidates the old Direct/Proxy evidence for that process birth. A carrier
incarnation change invalidates a Proxy that names the old carrier process. Stale evidence is never used for a
new process birth.

### J. Carrier connection loss invalidates Proxy

Drop the carrier's active connection to the destination. The carrier refuses the next carried call with
`connection-lost`; the source writes Proxy Disconnected with that reason, stops selecting that Proxy, and
either establishes another valid Proxy or exposes no active route.

### K. Sent request is never rerouted

Force a post-commit answer timeout. Assert the request is not resent through another connection and the timeout
alone does not invalidate a working Proxy.

### L. #2640 independence

Switch effective reachability Direct -> Proxy -> Direct without changing the semantic destination. Assert no
`orgs.topics` generation, route-barrier ACK, or watermark transition is created by those connection changes.

---

## 14. Ownership and RDM target

RDM owns the generic Direct/Proxy connection model, incarnation-bound reachability, the return
to Direct and the protocol carrier-policy hook (`rdm-transport/src/connections.rs`,
`rdm-node/src/connection_reconnect.rs`). An application owns its protocol descriptors and its carrier
policy; Rafka's, and where its copies lived before i148, are Rafka's pages.

---

## 15. Invariants

1. `ops.nodes.connections` is the one durable history of node-to-node connection facts.
2. The subsystem is generic across every node type; broker routing is only one consumer.
3. Direct and Proxy are first-class connection kinds in `NodeConnection`.
4. Connected/Disconnected/Failed map to active/dead/failed virtual classifications.
5. The raw log keeps event history; current indexes keep one latest Direct and one latest Proxy per pair.
6. The current index never grows one member per retry attempt.
7. A node holds its own connection facts only, from the writes it made; it reads none from the store (Q227).
8. Active Proxy state is source-owned and consumed by that source's effective route projection.
9. Failed and Dead retry history is durable and indexed; the mesh-primary's node investigation reads it when it runs, and no other node holds it.
10. A valid active Proxy overrides Direct until the Proxy is durably deleted.
11. Every node owns repair of its own direct connections in node-base.
12. Node-admin does not schedule or command local direct reconnect attempts.
13. No connection retries on its own: every dial is a call on `Send` (Q344, Q348).
14. A Direct Failed row records the last dial whose attempts were spent; nothing is scheduled from it.
15. A restart closes every connection the node held; the restarted node reads no row back (Q227).
16. Only the source's own Direct Connected creates the proxy-delete obligation; an accepted inbound connection from the destination is a recovery trigger (§13 G).
17. Failed proxy-delete writes are retried; routing safely remains on Proxy until the delete lands.
18. Destination and carrier process incarnations make stale route evidence refusable by name.
19. A failed direct attempt is source-local reachability evidence, not a sign the node is dead anywhere.
20. Customer traffic does not periodically probe Direct while a durable Proxy is active.
21. A sent/committed invocation is never silently rerouted or replayed.
22. Connection routing is independent of `orgs.topics` semantic-route convergence and #2640.
23. The `NodeConnection` row change carries `durable format moved` and requires the established full rebirth.
24. #2722 is authorized to make exactly the mesh/transport changes required by this design.
