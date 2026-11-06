## Q383: a full send queue loses a channel burst and its repair. May the iroh-gossip fork change, and to what?

**Ruled, Luke 2026-11-06:** "move all the to the vendor tip overlay our fixes where there is still a gap then re-run".

Evidence, scripts and the temporary test code: `rust-distributed-mesh-deploy/reports/burst-loss-2026-11-06/`. L1 unless stated: 20 nodes, one host, loopback.

### 1. Spec

`gossip.md` §7: "Ordinary lost pushes are repaired by iroh-gossip Plumtree mechanisms." and "The pinned iroh-gossip nonblocking-send fix must ensure one full peer queue cannot block the actor. A dropped full-queue enqueue is recovered through ordinary lazy-push/GRAFT behavior." Owner: the node, in its iroh-gossip net actor. Sequence: enqueue dropped → IHave reaches the peer → the peer's Graft → the message resent.

Fork ledger `iroh-noq-netwatch-gossip-fixes.md`, on the patch: "Remove when an upstream release never awaits a peer's send there and our regression tests pass against it."

### 2. Issue

M11, 5,000 x 256 B, delivered of published: 16.5-24.7% in 5 of 5 runs at fork `4d58243`; 100.0% in the one run so far at upstream `v0.102.0`.

### 3. Flame

No span's fields contradict each other. The lead is the publish span against the receipts: at the fork, `outcome=handed-to-gossip neighbours=5` on sequence 100 and no receipt of it anywhere.

Fork `4d58243`, RDM `3f2e731dd`: http://192.168.68.99:16687/trace/928652ff8755e7d85bf02a4065b225c4

```
rdm.mesh.channel.update.via-publish      5000   19:42:15.659276 .. 19:42:15.716816
rdm.testkit.channel.update.via-received 15824   19:42:15.749663 .. 19:42:15.988425
{'chunks':'1','outcome':'handed-to-gossip','bytes':'591','sequence':'Some(0)','neighbours':'5','event_type':'testkit.app', …}
```

First receipt 33 ms after the last publish, +90 ms from the first: 5 ms IHave dispatch plus the 80 ms Graft timer. 938 distinct messages received; 18 of 19 receivers hold exactly 827, in runs `0-63`, `576-639`, `1216-1279`, `3201-3264`, `3566-3629`, `3682-3745`, `4030-4093`, `4606-4669` (each 64), `1796-1918`, `2319-2621`.

Upstream `v0.102.0` over the iroh and noq forks, RDM `a0e612f24`: http://192.168.68.99:16687/trace/2c44384ae74f001f817336e2a0ea5a00

```
rdm.mesh.channel.update.via-publish      5000   23:29:01.857843 .. 23:29:02.074715
rdm.testkit.channel.update.via-received 79889   23:29:01.859101 .. 23:29:02.205886
```

First receipt +1.3 ms. All 5,000 distinct messages received; per receiver 2,691-5,000. 231 `via-lagged` spans in that run.

The Graft and the drop are not spans (`span_export_filter` turns third-party crates off). Iroh's own debug log at the fork, consecutive lines on one node (`verbatim-graft-drops.txt`):

```
2026-11-06T22:49:25.374026Z DEBUG gossip{me=57afa7517b}: iroh_gossip::net: handle out_event event=SendMessage(PublicKey(45c58dac249e15bca4ce2903aafd820175eae7f717444054f2a78c6c788854f1), Message { topic: TopicId(ade4ef756cc26699590f825d99cf3e7caa9bce344eebc2bfd37f308be7c1701b), message: Gossip(Graft(Graft { id: Some(MessageId(70e433c01bab633aaf3b6f2807d70e9ef8fb8f3ed456aac303507719b75b64d4)), round: Round(4) })) })
2026-11-06T22:49:25.374028Z DEBUG gossip{me=57afa7517b}: iroh_gossip::net: send queue full, dropping message peer=45c58dac24
```

### 4. Observations (fork `4d58243`, upstream `a16f016c` = `v0.102.0`, RDM `a0e612f24`)

The fork:

- [code] `net.rs:823`, `:826`: `try_send`; on `Full` the message is counted and gone. `net.rs:49`, `:572`: 64 slots per peer, shared by every kind of message and every topic. Patch `637a374`.
- [code] `plumtree.rs:790`: `add_lazy` has no floor; `:583`, `:669` call it on a duplicate and on a Prune. A node can hold no eager peer.
- [code] `plumtree.rs:692`: one Graft timer per message id. `net.rs:501`: every expired timer runs in one loop. `plumtree.rs:700`: `on_send_graft_timer` never learns its Graft was dropped.
- [log] One burst, per message, on 19 of 20 nodes (`chain-debug-run.txt`): eager set 0 of 6 at its start; of the 2,822 messages no receiver got, 2,783 had every Graft dropped, 23 lost the reply, 16 lost their eager pushes and Grafts. Four neighbours kept Grafts for the same indices.
- [run] 24 bursts, no debug log (`controlled-run.txt`): eager 0.00 at the start, 7 bursts, 640-1,242 of 5,000 reached any receiver. One message and 300 ms first, 8 bursts, eager 0.91-4.59, 4,217-4,960, lowest receiver 3,482.
- [run] `small-bursts.txt`, from eager 0.00: 64 → 64; 100, 200 and 500 → exactly 64 on every receiver.

Upstream `v0.102.0`, the same test in the same lane, with no RDM source change (`upstream-v0.102.0-bursts.txt`, `upstream-account-debug-run.txt`):

- [code] `net/queue.rs:36`: 1 MiB per peer, by bytes. `net.rs:908`: data and announcements that do not fit are dropped and counted. `proto/topic.rs:379`: a dropped one is announced again. `net.rs:918`: a control message that does not fit closes the connection. `net.rs` `QueueOptions` docs: `broadcast` waits up to 100 ms per message once queues are half full.
- [run] 28 bursts: every message reached a receiver in all 28. Eager never 0, lowest 2.85. Queue drops 193 in one burst, 0 in 27. Connections closed 0. Stale, true-offline and investigated spans 0; every node ready. 100, 200 and 500 from a rested publisher delivered in full, the lowest receiver 442 of 500 once.
- [log] Three 5,000 bursts, per message: 18 of 18 logged receivers held all 5,000 at the protocol, 90,000 of 90,000.
- [run] M11's row, one run, the old soak running for part of it: 5,000 → 100.0%, p50 61.7 ms (fork 90-179 ms); 20 x 256 B p50 1.5 ms (fork 88-91 ms); 1,000 x 32 KB 87.0% (fork 67.4-83.7%).

Limits and siblings:

- [run] Not fixed: a node gets a message and RDM's subscriber misses it. On upstream, 1,189-3,387 receipts per 5,000 burst over 18 nodes, worst node 849; lowest receiver 2,193-4,700 with no debug log. RDM's 1,024-slot queue reported 0 full. `TOPIC_EVENT_CAP` is 256 in both (`net.rs:55`).
- [run] M12 Direct throughput read lower in the one run: 64 streams 229 → 99 MB/s, 16 x 1 MB 573 → 374 MB/s. Gossip is not in that path and the soak was running; the cause is not established. The load plan is running on a quiet box.
- [code] Vendor pins against upstream main today: iroh `076e1a46` 25 ahead, lacks 10; noq `bf723891` 2 ahead, lacks 12; netwatch `5739114c` 5 ahead, lacks 3; gossip on main `4d58243` 31 ahead, lacks 15. The commits lacked include noq #866, #867, #884, #835, #868 and iroh #4621. None of these has been tested here.
- Not run at upstream: RDM's fast and release gates, the ledger's regression tests, Rafka. The lane used `[patch.crates-io]` for iroh, iroh-base and noq, which holds only in the building workspace; Rafka would need the same lines, a merge into our fork would not.

#### Inferred

- [inferred] Upstream's eager set stays non-empty because nothing is lost and #207 keeps grafted links: observed never 0, not attributed to one commit.
- [inferred] The subscriber shortfall is the 256-slot topic channel's lag: `via-lagged` moves and RDM's queue stays clear. Whose loop falls behind is not measured.
- [inferred] A publisher's eager set empties only after a burst that dropped messages: seen after 12,000/s and over, never in 12 readings after paced traffic up to 8,000/s.

### 5. Delta

Spec says a dropped full-queue enqueue is recovered through lazy-push/GRAFT; fork `4d58243` sends the Graft and its reply through the same full queue and forgets them, so 2,783 of 5,000 messages reached no receiver in one burst, where upstream `v0.102.0` delivered every one.

### 6. Arms

Every arm assumes the net actor never awaits a peer's queue.

- A: merge upstream `v0.102.0` into our gossip fork, removing `637a374`, and move the pin — changes the gossip fork, RDM's pin, Rafka's pin. Measured as upstream over our forks, not as the merge.
- B: arm A, and merge upstream's tips of iroh, noq and netwatch into our forks, keeping our patches — changes all four forks and every pin. Not measured.
- C: raise `SEND_QUEUE_CAP` at `4d58243` — changes one fork constant. Not measured.
- D: `Publisher::publish` holds to a measured rate — changes RDM's publisher, no fork. Forwarding nodes' queues and two publishers on one link stay exposed. Not measured beyond paced bursts.
- E: no code: §7 and the consumer guide state the limit, and Rafka paces itself — changes page text, with Luke's yes on the exact words.
- X: something else.

dev recommendation: B, gossip first and each vendor proved in a lane before anything is pushed to a fork: the load plan after each, and if a vendor's merge will not go green, stop at what is green, which is arm A at the least. This replaces my earlier recommendation of a bigger queue, written before upstream was tested. The subscriber shortfall is a separate item no arm fixes; it comes next on its own number.
