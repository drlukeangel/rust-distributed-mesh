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
3. The pages: gossip.md §5, "after the accepted join the birth joins its mesh channel"; §8 (:871-882), the
   node-admin "joins every type it does not already hold before it answers the join", and a joining node's
   "first neighbour is its own mesh's node-admin, already joined"; §6, the admin dials a held member only as
   repair, once its coverage has gone stale for the repair window. Nothing on the pages has the admin dial a
   fresh joiner at its join. The guarantee broken is §8 (:905-909): "Live frames and pulled rows share one
   apply rule, so nothing gossiped during the pull is lost"; a broadcast into the zero-neighbour gap has no
   Plumtree neighbour to push to (§7), so the gap itself is the defect (Arch).

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

**X1, telemetry only (no behaviour change; approved by Arch).** On main's code (E1 reverted too), one span
on every site where RDM hands peers to iroh-gossip: `rdm.mesh.connection.update.via-join-peers` (node,
topic, peers, side, caller), the caller a `&'static str` set at the call site. The sites (Arch, git grep
at `c7102594`):
- rdm-transport: `gossip.rs:514` (the app hub's learn), `gossip.rs:578` (the app-channel subscribe with
  peers), `membership.rs:1575` and `:1715` (the mesh and backbone subscribe and reopen with peers),
  `membership.rs:1788` (the channel `join_peers`), `membership.rs:1830` and `:1863` (the repair and refeed
  paths' direct `join_peers`).
- rdm-admin: `fabric_builds.rs:163`, `:319`, `:421`, `:452` (the Build topic, straight on iroh-gossip).
- through `Membership::join_peers`, named by their caller: `admin/boot.rs:1839`, `:2551`;
  `admin/session.rs:575`, `:704`; `reenter.rs:161`; rdm-node `node.rs:1075` (the joiner's mesh join),
  `:1462`.
The iroh debug layer is kept.
- To prove: for every dial at a join, which RDM caller made it, on which side, for which topic, and its
  time relative to the joiner's seed dial. Also, in the red run, which connection iroh closed, and why.
- Run: 1 run, read it in full; then a cohort of 4 runs, until one is red, read.

**X2, the change, chosen from X1's data (expected, not decided).** Conformance, not a new rule: the caller
X1 shows dialing the joiner at its join conforms to §8/§5/§6. Suspects, the same for Arch and the lead:
`admin/boot.rs:1839`'s app-types learn with the joiner as a bootstrap peer (conformance: subscribe without
the joiner as a peer) and `admin/boot.rs:2551`'s round dialing every member (conformance: dial a member only
under §6's stale-coverage rule). Written at the caller's file:line with the page lines it conforms to; the
exact change goes to Luke with X1's flame through Arch, as one ruling, before it lands.
- To prove: no double dial, no neighbour-down within 50 ms of its neighbour-up, and the cell green.
- Run: 1 run; then a cohort of 4; then, only if both are clean, 30.
- Success: double dial 0/30, early drop 0/30, red 0/30, against the baseline above (red about 1 in 9,
  double dial in most runs).

**Out of scope:** any change in iroh or iroh-gossip; an app-channel re-feed or redelivery machinery
(gossip is best-effort, the start read is the truth).

## 4a. X1 results (2026-10-19; telemetry only, branch `e4s8-x1-dial-spans`, `8d14b584`, not landed)

Process followed: 1 run read in full (green), then a cohort of 4 (run 4 red), then the cohort repeated
with OTLP export until a red (run 1 red). The demo consumer workspace was built against lane e4s8's
crates; the iroh debug layer was on.

**The red, exported to Jaeger.** Times are from the first neighbour-down:

| t | span | caller | side | channel | peers | trace |
|---|---|---|---|---|---|---|
| −14.8 ms | via-join-peers | `apps.subscribe` | admin | demo.large-records | the broker | [741cf0ac](http://192.168.68.99:16687/trace/741cf0acc0ad2f69c34c3cae5c387d02) |
| −14.6 ms | via-join-peers | `apps.subscribe` | admin | demo.records | the broker | 741cf0ac |
| −14.5/−14.4 ms | via-join-peers | `apps.learn` | admin | both | the broker | 741cf0ac |
| −13.3/−13.2 ms | via-join-peers | `apps.learn` | admin | both | the broker | [bbeb08f2](http://192.168.68.99:16687/trace/bbeb08f25fd0d9fab3187a990fc6a304) |
| −9.8 ms | via-join-peers | `membership.subscribe` | broker | mesh:mesh1 | its seed, the admin | [cf172d3f](http://192.168.68.99:16687/trace/cf172d3f1dcacc9d7f124a92272744d7) |
| −9.6/−9.5 ms | via-join-peers | `apps.subscribe` | broker | both | the admin | |
| 0 ms | via-neighbour-down | | admin | demo.records, then demo.large-records | | [beb7d942](http://192.168.68.99:16687/trace/beb7d942d4f5b03cace5f8dcb003a728) |

Trace 741cf0ac is the broker's own boot-and-join trace (71 spans): the admin's first dial runs inside
its serving of the broker's join. Trace bbeb08f2 is the admin's app-types loop (`via-types-read`).

**The same red, without export (cohort run 4), with iroh's log on one clock:** the admin's
`apps.subscribe` and `apps.learn` at −14.1 ms, iroh on the admin "start to dial" the broker at −13.8 ms;
`apps.learn` again at −13.1 ms; `admin.round` on mesh:mesh1 at −12.5 ms; the broker's
`membership.subscribe` and `apps.subscribe` at −9.4 ms, iroh on the broker "start to dial" the admin
at −9.0 ms; each side then holds a Dial and an Accept connection; one closes at −1.1 ms; "active send
connection closed, mark peer as disconnected" on both sides at 0.0 and 0.3 ms; neighbour-down on all
three topics on both sides at 0.4-0.5 ms.

**A green run (run 1)** has the same admin dials at 0 ms and the broker's at 3.5 ms; there the second
connection's close does not drop the peer. `admin.round` came at 99 ms, over the connection that already
existed.

**The callers named.** Three admin-side callers dial a fresh joiner at its join, all before the
joiner's own seed dial:
1. the join door's app-types hook, `admin/boot.rs:1447` (`learn_app_types_from` with the joiner's
   gossip address, so `Hub::learn` records it as a joined peer, `Hub::ensure` subscribes with it as a
   bootstrap peer, and `join_peers` asks it): `apps.subscribe`, `apps.learn`;
2. the app-types loop's row branch, `admin/boot.rs:~1600` (`learn_app_types_from` for a member of the
   admin's own mesh): `apps.learn`;
3. the admin round, `admin/boot.rs:2551` (`join_peers(mine)`, every member of its mesh not yet asked):
   `admin.round`.
The first to run makes the dial; the others reuse it. That is why E1 (1 removed) and E3 (1 and 2 removed)
changed nothing: the next caller made the same dial before the joiner's.

## 4b. X2 as it now stands (for the ruling; not run, not landed)

One rule, conformance to the pages at the three callers X1 named: the admin does not dial a fresh
joiner of its own mesh at its join.
- `admin/boot.rs:1447`: hold the joiner's types and subscribe them with no joiner as a peer (§8
  :871-882: the admin joins the types before it answers the join; the joiner, answered, joins its
  channels, its first neighbour the admin, already joined).
- `admin/boot.rs:~1600`, row branch: the same, for a member of the admin's own mesh (§8).
- `admin/boot.rs:2551`: the round dials no member; §6's stale-coverage repair (`membership.rs`
  `RepairSchedule`, one attempt per member per repair window) is the admin's only dial to its members
  (§6: a held member is dialled only once its coverage has gone stale for the repair window).
Proof run: 1 run, a cohort of 4, then 30, each with the X1 spans and the iroh layer. Success: no double
dial, no neighbour-down within 50 ms of its neighbour-up, the cell green.

## 5. Asked of Arch (answered 2026-10-19)

Arch: §2 is right in substance (citations corrected above); X1 approved with the full site list; X2 matches
the pages, stated as conformance, and goes to Luke with X1's flame before it lands.

The questions were:

1. Is the reading in §2 right, and is X1's instrumentation enough to name the dialing callers?
2. Does X2's rule, that the joiner dials and the admin of its own mesh does not dial it at the join, match
   gossip.md §5 as you read it, or does the page intend something else?
