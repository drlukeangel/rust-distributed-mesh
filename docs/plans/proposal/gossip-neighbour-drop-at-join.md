# Proposal: a joining node's gossip neighbour drops 2-4 ms after it forms

**Status:** experiment proposal, for Arch's feedback before any change. Q84 (Luke, 2026-10-19): fix it in
RDM's own gossip code, no iroh change. Nothing below has landed: E1-E3 are reverted (main is `7ae3aa83`;
`fb4017de` exists only on lane e4s8's local branch `e4s8-join-holds-no-dial`). The iroh debug layer may be
kept as test tooling (Luke). Author: the lead, 2026-10-19.

## 1. What we see (telemetry)

**The red.** The release at `7ae3aa83` failed `demo-rshape-consumer-app_channels`
(`demo/tests/app_channels.rs:65`, "snapshot: Indeterminate(ReplyDeadline)", then `:49`, the 60 s
`recv` bound elapsed). The broker's first `demo.records` event never reaches the root admin, so the
cell's pull, which waits for that event, times out.

**Not a regression.** It runs at the same rate before the commit that first showed it:

| commit | runs | red |
|---|---|---|
| `28df61d0` | 17 | 1 (run 17, the RDM dev) |
| `7ae3aa83` | 18 | 3 (the lead 2/8 in e4s3, 1/10 in e4s8) |

**The spans (RDM's own, 28df61d0 run 17).** Times are from the broker's publish:
- −12/−11 ms: `rdm.mesh.channel.update.via-neighbour` for `demo.records` and `demo.large-records`, and
  `rdm.mesh.connection.update.via-neighbour-up` for `mesh:mesh1`, on both the broker and the admin.
- −10/−8 ms: `via-neighbour-down` on all three topics, on both sides, `neighbours=0`, with no node-rpc
  connection close near it. Jaeger:
  - broker side: http://192.168.68.99:16687/trace/f85a6be1a45d2b68e3f833bdd75a6a39
  - admin side: http://192.168.68.99:16687/trace/4d9e5ee5bda234e5ec0a04b834931b37
- 0 ms: `rdm.mesh.channel.update.via-publish event_type=demo.records neighbours=0
  outcome=handed-to-gossip` (http://192.168.68.99:16687/trace/9ba93ba206d65544e272f80339abae71). The event
  reaches no one.
- `mesh:mesh1` comes back (its no-neighbours re-feed, gossip.md §6). The app channels have no re-feed;
  they re-form later, and the event published in the gap is never delivered.

Across 68 runs with no RDM change, 6 had a neighbour-down within 50 ms of its neighbour-up.

**iroh-gossip's own log (a local debug layer, run gd-9, red).** `RDM_DEBUG_GOSSIP=1` adds a stderr
layer for `iroh_gossip` events to `EvidenceCapture` (uncommitted tooling in lane e4s8):
- `00:15:19.575300` admin `bb981ad164`: `JoinPeers([39d84d097b])` on topic `2ba8ecc3` (its Key(0)),
  then on the app topic `5845a85b`; `start to dial peer=39d84d097b`.
- `00:15:19.579897` broker `39d84d097b`: `start to dial peer=bb981ad164` (its seed).
- Each side then holds two connections to the other (`connection established origin=Dial` and
  `origin=Accept`).
- `00:15:19.590178` one is closed. On the admin: "active send connection closed, mark peer as
  disconnected" → `PeerDisconnected` → hyparview "removed from active view, reason: ConnectionClosed"
  on every topic, while "remaining 1 other connections".

## 2. What is valid, and what is not yet proven

Valid, from the telemetry above:
1. At a join, the admin and the joiner dial each other within about 5 ms, and the pair ends up with two
   gossip connections.
2. When iroh-gossip closes one, it marks the peer disconnected and drops it from every topic, though the
   other connection stays up. A broadcast in that gap is lost. The app channels have no re-feed to
   recover.
3. The page (gossip.md §5): after the accepted join the birth joins its channels, "its first neighbour is
   its own mesh's node-admin, already joined"; the node-admin "joins every type it does not already hold
   before it answers the join". Nothing on the page has the admin dial the joiner.

Not yet proven:
1. **Which RDM call sites dial at a join.** RDM records no span on a gossip dial. Two admin-side dials are
   seen in iroh's log: on the mesh topic (gd-9, Key(0)) and on the app topics (gx-15, Key(3) and
   Key(4), right after the admin subscribes them). Their RDM callers are inferred from code, not
   measured: the admin round's `membership.join_peers(mine)` (`admin/boot.rs:2551`), and the
   app-types loop's `learn_app_types_from` (`admin/boot.rs:~1600`, `via-types-read`).
2. **Why a double dial does not always drop the peer.** Double dials are common (7 of 9 runs in gd), but
   most runs stay green. What decides which connection iroh closes, and when that drops the peer, is not
   known.

## 3. Experiments already run (outside this process, recorded as they were)

Each was a change made on a hypothesis before the dial sites were measured. None landed.

| # | change | runs | red | double dial | read |
|---|---|---|---|---|---|
| E1 | `fb4017de`: the join door holds the joiner's app types and dials nothing | 60 | 4 | — | no effect: early drops 6/60, as before |
| E2 | E1 + the join door marks the joiner asked on the mesh channel (`hold_joiner`) | 30 | 12 | 26 | worse; reverted |
| E3 | E1 + the app-types loop's row branch holds and dials nothing; E2 reverted | 24 (stopped) | 9 | 14 | no better; reverted |
| — | E1 only, with the debug layer | 9 | 1 | 7 | the baseline for the iroh log above |

Lesson: a fix chosen before the dial sites are measured is a guess. E2 and E3 raised the red rate, so
there is at least one effect that isn't understood yet.

## 4. Proposed experiments

**X1, telemetry only (no behaviour change).** On main's code (E1 reverted too), add one span on every
gossip dial RDM asks for: `rdm.mesh.connection.update.via-join-peers` (node, channel or topic, peers,
caller). The callers are: the channel `join_peers` (`membership.rs:1780`), the app hub's `learn`
(`gossip.rs:514`), and a subscribe with bootstrap peers (`gossip.rs:578`, the mesh and backbone
subscribes). Keep the iroh debug layer.
- To prove: for every dial at a join, which RDM caller made it, on which side, for which topic, and its
  time relative to the joiner's seed dial. Also, in the red run, which connection iroh closed, and why.
- Run: 1 run, read it in full; then a cohort of 4 runs, until one is red, read.

**X2, the change, chosen from X1's data (expected, not decided).** If X1 shows the admin's dials at a
join come from a caller the page does not give the admin (§5: the joiner dials; the admin is already
joined), that caller stops dialing a joiner of the admin's own mesh. This is one change, at the caller
X1 names. Repair stays as §6 has it.
- To prove: no double dial, no neighbour-down within 50 ms of its neighbour-up, and the cell green.
- Run: 1 run; then a cohort of 4; then, only if both are clean, 30.
- Success: double dial 0/30, early drop 0/30, red 0/30, against the baseline above (red about 1 in 9,
  double dial in most runs).

**Out of scope:** any change in iroh or iroh-gossip; an app-channel re-feed or redelivery machinery
(gossip is best-effort, the start read is the truth).

## 5. Asked of Arch

1. Is the reading in §2 right, and is X1's instrumentation enough to name the dialing callers?
2. Does X2's rule, that the joiner dials and the admin of its own mesh does not dial it at the join, match
   gossip.md §5 as you read it, or does the page intend something else?
