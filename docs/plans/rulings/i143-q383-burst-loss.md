## Q383: a full send queue loses a channel burst and its repair; what changes?

Evidence, scripts and the experiment patch (never committed): `rust-distributed-mesh-deploy/reports/burst-loss-2026-11-06/`.
L1, 20 nodes, one host, loopback, iroh-gossip fork `4d58243`, RDM `a0e612f24`.

### 1. Spec

`gossip.md` §7: "Ordinary lost pushes are repaired by iroh-gossip Plumtree mechanisms." and "The pinned iroh-gossip nonblocking-send fix must ensure one full peer queue cannot block the actor. A dropped full-queue enqueue is recovered through ordinary lazy-push/GRAFT behavior." Owner: the fork's net actor. Sequence: enqueue dropped → an IHave reaches the peer → the peer's Graft → the message resent.

### 2. Issue

A 5,000 x 256 B burst on an application channel delivers 13-25% when the publisher's eager set is empty at its start, 7 of 7 such bursts in the controlled run; M11 at L1 read 16-25%.

### 3. Flame

M11's exported run at `3f2e731dd`, the 5,000 burst: http://192.168.68.99:16687/trace/928652ff8755e7d85bf02a4065b225c4 is one message that survived, `m11-2/0`. Across that burst ClickHouse holds receipts for 938 distinct messages: 18 of 19 receivers hold exactly 827 and one 938, in runs `0-63`, `576-639`, `1216-1279`, `3201-3264`, `3566-3629`, `3682-3745`, `4030-4093`, `4606-4669` (each 64), `1796-1918`, `2319-2621`. M11's row: 24.66% at the far node, publish to receipt p50 90.5 ms against 5.6 ms for the 1,000 burst: 5 ms IHave dispatch plus the 80 ms Graft timer.

The Graft and the drop are not spans: `span_export_filter` turns every third-party crate off. The evidence for them is iroh's own debug log (`RUST_LOG=iroh_gossip::net=debug`) on every node but the first node-admin, the line after a `SendMessage` naming its drop:

```
handle out_event event=SendMessage(PublicKey(4cf7e53c…), Message { topic: TopicId(70986f4c…), message: Gossip(Graft(Graft { id: Some(MessageId(c3eea78f…)), round: Round(0) })) })
send queue full, dropping message peer=4cf7e53c64
```

The raw logs (1.7 GB) were removed after the account was taken; the account is `chain-debug-run.txt` and `account-debug-run.txt`. Burst `max-3`, 5,000 messages in 109 ms:

```
broadcasts before the first eager push: 4385
first Graft reached the publisher at +90.4 ms
messages by how many receivers got them at the protocol: [(0, 2822), (1, 156), (18, 1458), (19, 564)]
(Grafts attempted, kept in a queue, arrived): (4, 0, 0) 2783   (4, 1, 1) 23   (2, 0, 0) 16
8971yt7c: Grafts to the publisher 4690, kept 1092, dropped 3598
   per 5 ms (attempts/dropped): +85:214/150 +90:227/227 +95:266/202 +100:261/197
8971yt7c kept Grafts name indices: 0-63 441-504 707-770 968-1031 …
8zvwpyy3 kept Grafts name indices: 0-63 441-504 707-770 968-1031 …
```

### 4. Observations (fork `4d58243`, RDM `a0e612f24`)

- [code] `net.rs`, `OutEvent::SendMessage`: `active_send_tx.try_send`; on `Full` the message is counted in `msgs_dropped_send_queue_full` and gone. `SEND_QUEUE_CAP` is 64, per peer, shared by every kind of message and every topic. Fork commit `637a374` added this so the actor never awaits a peer.
- [code] `plumtree.rs`, `add_lazy` has no floor: a node can hold no eager peer. `on_prune` and a duplicate `Gossip` call it.
- [code] `on_ihave` schedules one `Timer::SendGraft` per message id. `on_send_graft_timer` pops an advertiser and sends one Graft; it never learns whether the send was dropped. With no advertiser left, it does nothing more. `on_graft` resends through the same `try_send`.
- [code] `net.rs`: the timer tick runs every expired timer in one loop (`pop_before`), so a batch of ids makes a batch of Grafts at one instant.
- [log] At `max-3`'s start the publisher's eager set was 0 of 6; the last change on each of its five neighbours' links, 14 s earlier at the end of the previous burst, was a Prune or a duplicate.
- [log] The per-burst account (`max-3`): 2,783 of the 2,822 lost messages had every Graft dropped; 23 had the reply dropped; 16 had their eager pushes and their Grafts dropped.
- [run] `controlled-run.txt`, 24 bursts with no debug logging, eager count read from `msgs_data_sent` over the first 500 messages. Eager 0.00 (7 bursts of the `none` arm): 640-1,242 reached any receiver, near-identical on every receiver, 65 of the 93 runs exactly 64. One earlier message and 300 ms (`warm` arm, 8 bursts): eager 0.91-4.59, 4,217-4,960 reached a receiver, the lowest receiver 3,482. Paced from eager 0.00: 12,000/s reached 4,669, 20,000/s 3,417 and 3,680.
- [run] `small-bursts.txt`, from eager 0.00: 64 → 64, 100 → 64, 200 → 64, 500 → 64, on every receiver. From eager ≥ 1, reaching any receiver: 100 → 100, 200 → 200, 500 → 500, with one 456; the lowest receiver 148 of 200 and 326 of 500.
- [run] `paced-then-small.txt`: after paced traffic up to 8,000/s the eager set read ≥ 1 on 12 of 12 readings; every message delivered but 4 of 40,000 on one run. 12,000/s for 5 s: 59,155 of 60,000 reached a receiver. Experiment process killed by the kernel at 56 GB (its own held spans) at the second 12,000/s step; the readings printed before it stand.

#### Inferred

- [inferred] A burst with eager peers at its start loses by the same drop on more links: `max-5` began with 4 eager, fell to 0.65 per message at index 3,500 as repair duplicates pruned its links, and 235 of its 237 lost messages had every Graft dropped; forwarding nodes dropped 13,672 pushes and 26,270 Grafts. The per-message account is from the debug run, whose logging slowed bursts: 46k/s against 74-114k/s from an empty set, 19-21k/s against 28-38k/s from a non-empty one.
- [inferred] A receiver's further loss between protocol and subscriber is the 256-slot topic channel's lag, named by `via-lagged` (a0e612f24): 511 messages over 18 nodes in `max-5`, 2,944 in `max-2`, worst one node 756.

### 5. Delta

Spec says a dropped full-queue enqueue is recovered through lazy-push/GRAFT; code sends the Graft and its reply through the same full queue and forgets them, so 2,783 of 5,000 messages reached no receiver though the publisher advertised them to its neighbours (four observed; the first node-admin's log was not kept, and no Graft of its arrived for them).

### 6. Arms

Every arm assumes the net actor still never awaits a peer's queue (`637a374`'s reason stands). Arms A, B and C change the fork, which needs Luke's yes before any patch.

- A: raise `SEND_QUEUE_CAP` from 64 to a size that holds a burst's Grafts and replies, such as 8192 — changes one constant in the fork. Worst case per peer is the size times one gossip frame (4 KB), 32 MB at 8192. Past it the drop is as today.
- B: past the queue, an ordered list per peer with a byte bound; a peer past the bound is disconnected and the view replaces it — changes the fork's `Full` arm. Nothing is dropped while a peer's connection lives.
- C: a Graft or Graft reply whose send was dropped is retried — changes the fork's net actor and `plumtree.rs`, which today never learns of a drop.
- D: `Publisher::publish` holds to a measured rate — changes RDM's publisher, not the fork. A coping layer: forwarding nodes' queues and two publishers on one link stay exposed.
- E: no code: §7 and the consumer guide state the limit, and Rafka paces itself — changes page text only (Luke's yes on the exact text).
- X: something else.

dev recommendation: A, proved first in a lane with an uncommitted patch: M11 at L1 and L2 and the empty-set small bursts, before any pin moves. A is one constant and it tests the whole chain; B follows only if A's drop count or `via-lagged` rows still show loss. A does not change that a burst from a publisher with no eager peer starts 85 ms late (`add_lazy` has no floor); it stops that start losing messages.
