# Q227: connections.md text (#3229, lane 4)

The exact old and new text of every `docs/architecture/connections.md` passage Q227 changes (Luke, X: no node reads anything from the store on create, start or rejoin; the mesh-primary's node investigation reads connection facts when it runs). Line numbers are origin/main's (05cf16c52). The page in this branch carries the new text. Q230's lines (:329, :373-376, :408, :441-442, :682) are not touched here: #3230 owns their words.

The inbox named §3 carrier discovery (:273), §4.1 (:283-296), :378, :407 and requirements 7, 9, 14, 15. The page states the same reads in eight more places, listed here so none is hidden: §1 (:34-36), §3 (:229-231), §4 (:279), §4.2 (:303-312), §9 (:466-467), §9.3 (:516, :523-528), §10 (:589), §13 B, C and E. Requirement numbers are kept.

## 1. §1 (:34-36), who reads the indexes

Old:

````text
The raw topic remains an append-only history. Every event receives its own minted row id. The raw
history answers what happened; compacted declared indexes answer the latest state required by routing
and reconnect.
````

New:

````text
The raw topic remains an append-only history. Every event receives its own minted row id. The raw
history answers what happened; compacted declared indexes answer the latest state per pair, which the
mesh-primary's node investigation reads when it runs. No node reads its rows back to route or reconnect
(§4.1).
````

## 2. §3 (:229-231), the latest Direct row's fields

Old:

````text
Raw-log retention is therefore not part of reconnect correctness. The latest compacted Direct row carries
the recovery round, attempt ordinal and time needed to reconstruct backoff even after older raw events age
out.
````

New:

````text
Raw-log retention is therefore not part of reconnect correctness. The latest Direct row a source writes
carries the recovery round, attempt ordinal and time its next due time is computed from (§9.3).
````

## 3. §3.2 (:262-275), the conceptual consumers; carrier discovery (:273) goes

Old:

````text
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
````

New:

````text
This lets the mesh-primary's node investigation ask, when it runs ([node investigation](lifecycles/node-investigation.md)),
conceptually:

```text
leads:
    rtable(destination).filter(state=active).filter(kind=direct)
```

Routing, reconnect and the carrier pick read no row: a node holds its own facts from the moment each
write lands (§4.1). A read of other nodes' facts ahead of a Proxy or an investigation is stale the moment
it is read (Q227).
````

## 4. §4 (:279), what is held

Old:

````text
The fleet-wide held connections projection is routing state, not a distributed retry ledger.
````

New:

````text
Each node's held connections are routing state, not a distributed retry ledger.
````

## 5. §4.1 (:283-296), the fleet-wide Direct graph

Old:

````text
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
````

New:

````text
### 4.1 A node holds its own facts

A node holds the connection facts it writes, each from the moment its write lands: its latest Direct per
`(destination, alpn)` and its own active Proxy per `(destination, alpn)`. It holds no other node's facts,
and it reads none from the store on create, start or rejoin, or before a carrier pick (Q227). A node is
born holding no connection: a restart closes every connection the node held.

When the latest Direct record becomes Disconnected or Failed, the old active edge is removed from what the
node holds. The Dead/Failed row itself remains durable and indexed.
````

## 6. §4.2 (:303-312), the effective local routing view

Old:

````text
```text
global active Direct graph
        +
my active Proxy overrides
```

Failed and Dead Direct history is read from the durable latest-per-kind index by the owning source's
reconnect reconciler when needed. It is not gossiped into every node's resident routing graph.
````

New:

````text
```text
my latest Direct facts
        +
my active Proxy overrides
```

The owning source's reconnect reconciler takes its latest Failed Direct from what it holds (§9.3). No node
reads it back from the store, and it is not gossiped to any other node.
````

## 7. §6 (:378-386), carrier candidates

Old:

````text
Carrier candidates are active Direct edges from an eligible carrier to the exact destination. When several
are eligible, selection is deterministic and spread by source rather than "freshest wins" herd behavior.
Conceptually:

```text
stable_hash(source, destination, destination_incarnation)
    % deterministically_ordered_eligible_carriers.len()
```
````

New:

````text
Carrier candidates are the live nodes of the source's mesh, from the topology it holds, of a node type the
protocol lets carry, the source and the destination left out. The source asks them in path order,
deterministic and spread by source rather than "freshest wins" herd behavior, starting at:

```text
stable_hash(source, destination, destination_incarnation)
    % deterministically_ordered_candidates.len()
```

and wrapping. The carrier's own Direct to the destination decides: a candidate without an Active one
refuses by name (`carrier-edge-lost`, §8) and the source asks the next; when every candidate refuses, the
open ends with no active route. No connection fact is read to choose.
````

## 8. §7 step 2 (:407)

Old:

````text
2. It selects an eligible carrier from the active Direct graph.
````

New:

````text
2. It asks the candidates of §6 in their order; the first whose own Direct to the destination is Active
   answers.
````

## 9. §9 (:466-467), the reconciler's work

Old:

````text
This is deliberately **not** a normal platform `Job` entity. It is a local durable reconciler whose work is
reconstructed from `ops.nodes.connections`.
````

New:

````text
This is deliberately **not** a normal platform `Job` entity. It is a local reconciler whose work is derived
from the Direct facts the node writes and holds; it reads none back from `ops.nodes.connections`.
````

## 10. §9.3 (:516), the next due time

Old:

````text
The next due time is reconstructed deterministically from the latest indexed Failed record:
````

New:

````text
The next due time is computed deterministically from the latest Failed record the source holds:
````

## 11. §9.3 (:523-528), a restart

Old:

````text
A process restart therefore does not reset exponential backoff. The node reads its latest Direct-per-
destination index rows and reconstructs only the currently relevant schedule. It never replays the raw log.

The exact constants are implementation policy and must be tested, but the schedule is bounded, exponentially
backed off, jittered, and may continue at its maximum interval while the destination incarnation remains
relevant. A restart before `next_due` must not trigger an early attempt.
````

New:

````text
A restart closes every connection the node held, and the restarted node reads no row back (Q227): it
holds no reconnect series, and its next failed dial toward a destination is a new series at attempt 1.

The exact constants are implementation policy and must be tested, but the schedule is bounded, exponentially
backed off, jittered, and may continue at its maximum interval while the destination incarnation remains
relevant.
````

## 12. §10 (:589), a restart and the owed write

Old:

````text
After restart, the same pair of latest indexed rows reconstructs the pending retirement automatically.
````

New:

````text
A restarted node owes none: it holds no Proxy.
````

## 13. §13 B (:684-693)

Old:

````text
### B. Proxy survives source restart

With latest indexed rows:

```text
Direct -> Failed(E1,N)
Proxy  -> Connected(via=gw2)
```

restart gw1. The first operation after hydration uses the Proxy without rediscovering it.
````

New:

````text
### B. A restarted source holds no Proxy

With latest indexed rows:

```text
Direct -> Failed(E1,N)
Proxy  -> Connected(via=gw2)
```

restart gw1. The restarted gw1 holds no Direct and no Proxy and reads none back; its first operation dials
Direct and, on NotSent, proves its carrier again (§7). Forbidden: a route over a Proxy the restarted birth
did not prove.
````

## 14. §13 C (:695-698)

Old:

````text
### C. Failed reconnect backoff survives restart

Write `Direct Failed(E1, attempt=N, event_time=T)` such that `next_due` is in the future, restart the source,
and assert no attempt occurs before the reconstructed due time. Forbidden: restart at attempt 1.
````

New:

````text
### C. A restart starts reconnect backoff fresh

Write `Direct Failed(E1, attempt=N, event_time=T)`, restart the source, and assert the restarted source holds
no reconnect obligation from it and its next failure toward the destination starts a new series at attempt 1.
Forbidden: the restarted source reading the row back.
````

## 15. §13 E (:705-708)

Old:

````text
### E. Failed/Dead records do not pollute fleet-wide held routing

Generate repeated reconnect failures on gw1. Assert peer nodes' resident connection projection contains only
active Direct routing edges, never gw1's failed/dead retry history.
````

New:

````text
### E. Failed/Dead records stay with their source

Generate repeated reconnect failures on gw1. Assert no peer node holds any of gw1's facts: a node holds its
own facts only (§4.1).
````

## 16. §15 requirement 7 (:773)

Old:

````text
7. Fleet-wide resident connection state contains active Direct facts only.
````

New:

````text
7. A node holds its own connection facts only, from the writes it made; it reads none from the store (Q227).
````

## 17. §15 requirement 9 (:775)

Old:

````text
9. Failed and Dead retry history is durable/indexed and read on demand, not held fleet-wide.
````

New:

````text
9. Failed and Dead retry history is durable and indexed; the mesh-primary's node investigation reads it when it runs, and no other node holds it.
````

## 18. §15 requirement 14 (:780)

Old:

````text
14. The latest Direct Failed row is sufficient to reconstruct retry round, ordinal and next due time.
````

New:

````text
14. The latest Direct Failed row a source holds is sufficient to compute its retry round, ordinal and next due time.
````

## 19. §15 requirement 15 (:781)

Old:

````text
15. Restart never resets reconnect exponential backoff.
````

New:

````text
15. A restart closes every connection the node held; the restarted node reads no row back and its reconnect backoff starts fresh (Q227).
````
