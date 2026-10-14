# Node status, heartbeat and connections: proposals for review

**Proposal.** Luke asked for one doc covering the interlocking proposals; each is asked separately, one
ruling at a time, with its own number, recommendation and arms ending in "x: something else". §A records
what architect review settled. R-ST1 is taken as arm A (Luke: "I want status names now … one name means
the same thing everywhere"). R-HB1 follows Luke's direction of 2026-10-11 evening. R-CR5 is ruled A ("no just connections") and R-CR4 is ruled A ("draining is node.connections.delete"). R-HB3 is open. Doc text that rests on settled or taken items is applied to `docs/architecture/`; the
rest stays here.

## Why

- **A real bug.** mesh1.admin.2 was stopped while its mesh-primary held its heartbeat 13; the stop
  installed 13 as the mark. Heartbeat 14 (ReadyForTraffic, same incarnation, sent before the stop)
  arrived after it and ended the stop (gate b05c303b, node_stop admin cell: stop trace
  `1f019c9dc5c06a3cb9db7be229cb3340`; `rdm.mesh.membership.update.via-node-started` at seq 14 in trace
  `0164315174b5f3d478b34af76d519e94`). The defect is RDM's `stopped_refuses` in rafka-mesh-transport's
  membership.rs (RDM main a4e5df1d): a receiver marks its *own* last-heard sequence at the stop, and a
  later heartbeat that says ready ends the stop. iroh-gossip dedups and delivers but never orders, and the
  heartbeat races the direct Stopped reply.
- **Renames need a checkable state** (R-CR1a): "drained" waits for in-flight calls but closes nothing.
- **Names that mislead:** node.stop sets "Leaving"; mesh.delete sets "leaving" and "dead".

## A. Settled in review (the shape every ask below assumes)

1. **Heartbeat and status are separate facts on every path.** A heartbeat says only "this exact birth
   published again"; it never says started, connections-deleted or ready-for-traffic. The
   mesh-primary's status fact is the only thing that moves a member's status. Receivers hold both. A
   heartbeat with a higher number never clears stopped; deleted is terminal for the birth. This holds in
   Digests, Members aggregates, deltas, fulls, topology reads and join answers — otherwise another receive
   path rebuilds status from node-authored data. A receiver holding a heartbeat but no authoritative
   status keeps the birth non-routable (the states.md routing rule).
2. **`digest_seq` orders heartbeats and nothing else.** Gossip's four orderings stay; there is no
   per-author counter advanced on every message. A status fact carries the `topology_version` at which
   its subject changed (gossip.md's bump list already includes "a status that routing reads" and "an
   in_flight overlay added or cleared"; a heartbeat alone never bumps it). Receivers compare per subject.
3. **Author ordering reuses publisher epochs and the LeadershipView.** A new source primary begins a new
   epoch; a receiver installs a full of that epoch before its deltas; a forwarder that takes the seat
   publishes a full first. The one new contract is the handover step: the successor acquires the held
   member statuses and publishes a complete projection under its own epoch before any delta. One mesh
   has one publisher on the backbone; a frame received out of order, with an order key earlier than the
   newest held for its source, is ignored once, in the gossip receive path. Nothing checks who published
   it. The acting fabric-primary for a primaryless mesh uses the same path.
4. **A stopped birth publishes nothing.** At stop completion the node quiesces heartbeat publication (the
   stop gate, landed e8e86195). After node.stopped a heartbeat of that birth is not recognised and is
   refused by name; it never clears stopped. Accepted node.joined restores a stopped birth's membership
   (states.md), authored by the mesh-primary.
5. **R-CR3's request shape is kept; the workflow decides closure.** Standalone `All` is graceful
   node-wide closure (connections-deleting → connections-deleted). Selected-ID deletion closes those
   connections and records their drops; it sets no node status and does not make the node non-routable.
   node.stop composes graceful closure and the existing hard cut (stopping → stopped). Rename stays
   two-sided (R-CR1, R-CR2): the mesh-primary checks status, `ApplyNodeName` answers `Busy` naming held
   routes, and the condition holds through the name apply. Return to service is stop → start.
6. **Graceful completion is defined at the node, not per socket:** close admission to new application
   work, prevent new routing connections, let admitted work (and its downstream work) finish, close the
   covered connections, record their drops under the current name, return connections.deleted. Control
   streams stay usable; the call never closes its own reply path. The drain's Established / Deadline
   facts move onto the connections receipt beside the closed ids. The order: block admission of new
   application work; let admitted work finish, including its downstream connections (they belong to the
   same closure); wait for eligible work to reach zero; close all application routing connections; record
   drops under the current name; return connections.deleted. On the deadline the call returns incomplete
   closure with Deadline evidence — the remaining in-flight count and the connections still open — and the
   node stays connections-deleting; a new `node.connections.delete` attempt or node.stop leaves that state;
   node.stop may hard-cut, and its receipt keeps the deadline evidence. Graceful success is never reported
   after interrupting work, and connections-deleted is never published while a covered connection is open.
7. **The outer workflow owns the visible status;** child calls keep their events, spans and receipts.
   Standalone connections.delete shows connections-deleting → connections-deleted; node.stop shows stopping →
   stopped; node.delete shows deleting → deleted. A child's completion never replaces deleting with
   stopped. A failed start: the workflow restores the stopped condition and gets its receipt; until that
   rollback succeeds the start is failed, not stopped.
8. **Formats.** `BuildOperation` is `#[serde(tag = "op", rename_all = "snake_case")]` and `MemberStatus`
   is kebab-case, so variant renames are format changes wherever a plan or receipt is JSON. Every field
   added below is a named coordinated cut with its reseal (`durable format moved`, a rebirth; no old
   receipt is read, so no migration is owed — that is i139's). The envelope's reserved-position rules are
   kept.

## B. The asks (one at a time, in this order)

**Beta set (Luke 2026-10-11: "I want status names now … one name means the same thing everywhere"):**
R-HB1 (below, Luke's direction 2026-10-11 evening) is built first. R-CR5 (ruled A), R-CR4 (ruled A) and R-ST1 (arm A, everywhere)
follow as one coordinated cut. **Parked post-beta:** R-HB3.

### R-HB1: the mesh-primary authors and publishes every member status; a heartbeat is liveness only

Basis: §A.1 (settled in architect review) and Luke's direction on 2026-10-11 evening. The mesh-primary
executes every status change, writes it to its meta and publishes it, so it cannot hold a wrong value,
and a heartbeat never moves a status. This replaces the boundary design (a heartbeat number on Stopped
and NodeStopped). The own-mesh projection that was parked as R-HB2 is how it is built. It is not a
ruling text. The code still lands with its Jaeger flame (CLAUDE.md rule 1).

- **Liveness.** A heartbeat (Digest) says only that this exact birth published again. After
  node.stopped, a heartbeat of that birth is not recognised and is refused by name. Only the
  mesh-primary's node.joined (node.start) or node.delete ends a stop (node-start.md:31-32,
  gossip.md:290-294).
- **Ops are calls, not gossip.** node.joined, node.started, node.stopped and the other completions are
  calls to the mesh-primary and their replies.
- **Own-mesh projection.** The mesh-primary publishes its own member projection on its own mesh
  channel through the existing full and delta machinery, under its publisher epoch and
  `topology_version`. There is no new topic and no status counter. "Own-mesh members receive
  authoritative statuses through the primary's versioned member projection. Missing versions or an
  unknown source epoch trigger GetTopology repair. Receivers install the complete snapshot atomically;
  they do not queue unmatched status events." Peer meshes receive it as today, through Members and
  MembersDelta.
- **Field sources for an own-mesh birth.** Liveness, endpoints with freshness and the runtime fact come
  from the node's Digest. Status comes from the projection. A full therefore never overwrites fresher
  Digest endpoints.
- **Events vs statuses.**
  - NodeStopped, NodeDeleting and NodeRestarting stay immediate events.
  - The other statuses in R-ST1's list arrive only through the projection, on change, with no
    reinforcement frames. For a start, peers see pending, then ready-for-traffic (Luke: "wouldn't that
    just be pending and ready-for-traffic").
- **The version field.** Each status fact carries its subject's `topology_version`. It lands as a named
  coordinated schema cut of the lifecycle frames: every reader and writer in one landing, fixtures
  resealed, `durable format moved`.
- **Status recovery** is the existing top-up: a node missing a status fact pulls the versioned
  projection from its own mesh-primary with GetTopology.
- **Cost.** Every own-mesh member now processes its own mesh-primary's full and delta stream as well as
  the Digests. That is more traffic and work inside each mesh than today.
- **RDM.** `stopped_refuses` (rafka-mesh-transport membership.rs) loses "a later ready heartbeat ends a
  stop"; the node stops setting its own status.
- **Doc text, old/new.**
  - states.md "Who sets each state": the connections-deleting row is set by "the node itself"; under
    this it is set by the node's mesh-primary.
  - Add rows for starting, pending, connections-deleted, stopping and deleting, each set by the node's
    mesh-primary.
  - gossip.md §3.1: the Digest row lists `status`; under this it carries liveness only. The Members
    and MembersDelta rows gain the own-mesh channel publication and the field-source and
    events-vs-statuses rules.

### R-HB3 (parked post-beta): which replies carry the node's heartbeat number as evidence

Luke asked (2026-10-11) whether op messages could carry the sender's heartbeat number as a fallback for
"when did this come from you" — smaller than a rafka-time stamp. Under §A.2 it is contextual evidence
only, never an ordering: two status facts may carry the same number.
- **A:** every op message carries its sender's `digest_seq` (evidence; no receiver orders by it).
- **B (recommended):** a codec inventory — exactly the replies that feed a status fact, each an appended
  field on its 0x1B reply variant in the same coordinated cut: joined, started and connections-deleted
  carry the node's current `digest_seq`; Stopped carries its last `digest_seq` after the quiesce. Nothing on requests or other replies; provider-backed deletion carries
  nothing.
- **x:** something else.

B is recommended because the reviewers withdrew the blanket counter as a fifth ordering; these four
replies are where the mesh-primary takes the number it stamps on a status fact.

### R-CR4: the drain becomes the connections op — RULED A (Luke 2026-10-11, "draining is node.connections.delete")

§A.5–A.6 as the rule: `node.drain` / `node.drained`, the `DrainNode` / `NodeDrained` pair, the drain
receipt and the `drain-node:` step key retire; `node.connections.delete` carries graceful closure.
- **Rewrites R-CR1a (old/new, node-update.md "Rename precondition"):**
  - old: "the held status is stopped, or the node is drained and its `node.connections.delete` has
    completed (`node.connections.deleted`)"
  - new: "the held status is stopped, or its `node.connections.delete { scope: All }` has completed
    (connections-deleted)"
- **A (recommended):** as written. **B:** retire the live-not-routable status too (in-flight work cut,
  not finished). **C:** wording only. **x:** something else.

### R-CR5: what `All` cuts, and the rename rule with it — RULED A (Luke 2026-10-11, "no just connections")

R-CR3 defines `All` as the stop's cut, gossip neighbours included. A running node doing a standalone
graceful `All` would stop heartbeating, and node-recovery.md would mark it PendingReconnect and run the
ladder on a healthy node.

`All` defined in connections-log terms: every open Direct or Proxy entry of the node — its application
routing connections. Gossip neighbours and the Node RPC pool are not log entries, so they fall outside it
by R-CR2's own named facts (row, status, connections log), with no new classification. Selected-id
deletion stays maintenance, with no node-wide status.

Rename rule (replaces R-CR1a's wording): "Rename is permitted when the authoritative status is
connections-deleted or stopped and the node holds no application routing connections. Gossip and
necessary lifecycle/control connections are excluded from this check. New application routing
connections remain blocked through the name apply."

- **A (recommended):** `All` has one meaning everywhere — the log's application routing connections.
  node.stop composes two primitives: `node.connections.delete { All }`, then its own transport cut (gossip
  neighbours and the RPC pool). R-CR3 old: "`All` is the stop's cut (gossip neighbours and pooled peer
  connections)"; new: "`All` closes every open Direct or Proxy entry of the node; node.stop then cuts its
  transport (gossip neighbours and the RPC pool) as its own step."
- **B:** standalone `All` closes the log's connections only, while the stop's cut keeps the name `All` and
  also cuts gossip and the pool (two meanings of one word).
- **C:** standalone `All` cuts gossip and the silence marks exempt a connections-deleted birth (a coping
  layer).
- **x:** something else.

### R-ST1: status names follow their op

Rule: a status is named for the lifecycle op that sets it (`-ing` while it runs, `-ed` when it holds);
a sub-step gets no status; `pending` is the first state of the fabric and the mesh; `ready-for-traffic`
is the routing word; node.create includes node.start.

| entity | op | today | proposed |
|---|---|---|---|
| node | node.start (inside node.create or on a stopped node) | pending | **starting** |
| node | started, awaiting the mesh-primary | pending | **pending** |
| node | the mesh-primary's decision | ready-for-traffic | ready-for-traffic |
| node | node.connections.delete (R-CR4) | Draining | **connections-deleting → connections-deleted** |
| node | node.stop | Leaving | **stopping → stopped** |
| node | node.delete | overlay NodeDeleting, row Deleted | **deleting → deleted** |
| node | observed | row Dead, PendingReconnect | unchanged |
| mesh | mesh.delete | leaving → dead | **deleting → deleted** |
| fabric | fabric.create, rounds | pending → state-sync → state-commit → ready-for-traffic | unchanged |

Doc conflicts (old/new, states.md):
- line 12, old: "**Mesh:** `pending` → `ready-for-traffic` → `leaving` → `dead`. … dead is final." new:
  "`pending` → `ready-for-traffic` → `deleting` → `deleted`. … deleted is final." (also removes the clash
  with the node row's observer `Dead` mark)
- line 13, old: "pending → ready-for-traffic → Draining → Leaving … Stopped is Leaving with a live
  process; it is not a new status." new: "starting → pending → ready-for-traffic → connections-deleting →
  connections-deleted → stopping → stopped → deleting → deleted … stopped is a status: a live, stopped
  process."

- **A (recommended; Luke wants one name to mean the same thing everywhere):** as in the table, in docs,
  views, stored rows, the wire, gossip strings, spans and tests. **B:** docs and views only (two names for
  one thing on the wire). **x:** something else.

## C. Acceptance matrix (each a named cell with deterministic assertions and its trace)

| cell | asserts |
|---|---|
| late heartbeat after stop | a heartbeat of the stopped birth arriving after NodeStopped is refused by name; stopped holds until the mesh-primary's node.joined |
| late stopped after starting | a NodeStopped older than the starting fact (lower `topology_version`) is refused |
| two statuses between heartbeats | both apply in `topology_version` order; heartbeats move neither |
| primary handover | the successor publishes a full under its epoch before any delta; a frame with an earlier order key than the one held is ignored in the receive path |
| missed event repaired by top-up | a node missing a status fact gets it from GetTopology |
| graceful deadline | the connections receipt carries Deadline; connections-deleted is not published; stop hard-cuts and keeps the evidence |
| selected-id closure | named connections close, others stay open, no status change |
| reconnect racing rename | while the rename applies, no new connection to the node is admitted (the node holds no connection from the precondition through the apply); separately, a rename asked while a connection remains is refused with `Busy` naming it |
| failed start rollback | the workflow restores stopped with a receipted stop; until then the start reads failed |

## D. What changes (summary; counts in [rdm-code-conformance.md](rdm-code-conformance.md) §1)

| area | change | ask |
|---|---|---|
| wire / gossip | status out of the Digest; the mesh-primary's own-mesh status publication; `topology_version` on status facts | R-HB1 |
| wire | sender `digest_seq` as evidence on four 0x1B replies only: `StatusReply::Joined`, `Started`, `ConnectionsDeleted`, `Stopped` (B; nothing on requests or other replies) | R-HB3 |
| wire | `DrainNode` / `NodeDrained` retired; graceful closure on `node.connections.delete` | R-CR4 |
| gossip | standalone `All` keeps gossip membership | R-CR5 |
| rows, wire, gossip | status strings renamed; MeshStatus deleting / deleted | R-ST1 |
| RDM code | `stopped_refuses` loses "a later ready heartbeat ends a stop"; the node stops setting its own status; node.drain's code becomes graceful closure | R-HB1, R-CR4 |
| docs (Luke's) | states.md lists and "who sets"; gossip.md catalog; node-drain.md → a node-connections-delete page; node-update.md rename precondition | per ask |

## E. Order

R-HB1 is built first. R-CR5, R-CR4 and R-ST1 follow as one coordinated landing after their
rulings, each asked separately. R-HB3 is parked until a need shows. Each gossip change carries its Jaeger flame when its code lands (CLAUDE.md
rule 1); the proposal itself is reviewed without flames. Doc text (states.md rows, the handover step,
node-update.md's rename precondition, a node-connections-delete page in place of node-drain.md) waits for
Luke's consolidation pass.


