# iroh, noq, netwatch and iroh-gossip: the fixes we keep

The four forks RDM pins moved to their upstream tips on 2026-11-06 (Q383, Luke: "move all the to the
vendor tip overlay our fixes where there is still a gap then re-run"). Each is the branch
`rdm-upstream-2026-11-06` on drlukeangel's fork: upstream's tip with only the patches below on top. No
fork's default branch moved. The branch `rdm-upstream-2026-10-21` keeps each patch's own history, and
the hashes below are from it. This page lists every patch we still carry, why, and when it may go; and
every patch the move dropped because upstream now does the same. Upstream issues or PRs for the kept
patches: none filed yet.

This page is the one ledger of the forks: rafka-v2 keeps none (Luke, 2026-10-21; its
`docs/eng/vendor-divergence.md` is deleted and its `[patch]` comment points here). A change to a fork
lands here in the same commit as the pin that takes it, with the measurement that justified it.

| Fork | Pinned commit | Upstream tip | Our patches kept | Dropped (upstream does it) |
|---|---|---|---|---|
| noq | `9de1796d` | n0-computer/noq main `04807b67` (noq 1.5.0) | 1 | 0 |
| netwatch, portmapper | `24417e8d` | n0-computer/net-tools main `c07b688f` (0.20.0) | 4 | 0 |
| iroh, iroh-base | `938633e5` | n0-computer/iroh main `a5e39c44` (after v1.4.1) | 13 | 0 |
| iroh-gossip | `9a928f53` | n0-computer/iroh-gossip main `a16f016c` (v0.102.0) | 3 | 2 |

## noq

- **GSO halts only on a send that was segmented** (`d878a5ea`, GH#1044). A single-datagram send that
  fails with EIO or EINVAL (a loopback-bound socket sending to a LAN address) no longer turns
  segmentation offload off for every later send. Upstream still halts on any such failure.
  Remove when upstream conditions the halt on `effective_segment_size()`.

## netwatch and portmapper

- **UDP socket buffer size from `RAFKA_UDP_SOCKET_BUFFER_BYTES`, and `socket_buffer_sizes()`**
  (`a490499`). The override now defaults to upstream's 7 MB `SOCKET_BUFFER_SIZE`; the accessor reads
  the achieved sizes back. Upstream 0.20.0 adds `BindOptions::configure_socket`, which could carry the
  sizing instead: remove once iroh exposes that hook and RDM sets the size through it.
- **`get_state` enumerates interfaces on the blocking pool** (`ab1c3f0`), never on a runtime worker.
- **`interfaces::interface_enumerations()`** (`d3ee3c6`): the count the iroh fork's test asserts.
- **noq-udp by git from drlukeangel/noq** (`3b29b53`), so a git importer's graph holds one noq-udp.
- Dropped: `bind_raw`, unused since upstream's `BindOptions`.

## iroh and iroh-base

Each patch goes when upstream merges an equivalent, unless its line says otherwise.

- **`Endpoint::replace_direct_addrs` retires every superseded direct path**, open ones included (`f964f599c`).
  Remove when upstream retires every path of a replaced address set.
- **`Builder::network_monitor(bool)`, default true** (`921afe536`, R-B2): with false, bind builds no
  netmon monitor and enumerates no interfaces. Kept through the merge: `handle_network_change` still
  reads `if_state_details`, with upstream's #4605 no-server band-aid added in front. Remove when
  upstream accepts an endpoint option that binds without a network monitor.
- **`test-knobs` feature: `QuicTransportConfigBuilder::disable_nat_traversal_for_tests`** (`8bc873b79`, R-I3).
- **Cached mapped addresses evicted when a `RemoteStateActor` shuts down** (`c1d432c96`, `d82f4f13c`, `0f2af4c82`).
- **`RemoteStateActor` arms fed through an mpsc funnel** (`f5b6288e0`): no waker loss from
  `MergeUnbounded`/`FuturesUnordered::push`, which upstream still uses.
- **`RemoteStateActor` inbox capacity raised, with a drop metric** (`54196f708`).
- **`Builder::max_udp_payload_size`** for jumbo MTU (`32b293cbc`, U158).
- **`Endpoint::socket_buffer_sizes`** (`6895edb7e`, U13).
- **`send_datagrams` counter in `SocketMetrics`** (`bb1116d5f`, U12).
- **`poll_send` span at debug** (`986291499`) and the flow-bracket line at trace (`99b419a2c`).
- **cdylib crate-type dropped** (`bfcbca719`), so sccache caches the build. Remove when the crate type
  can be chosen per target (rust-lang/cargo#12260).
- **netwatch, portmapper and noq by git from drlukeangel's forks** (`5d248d3d4`), one of each in the graph.
  Remove with the last of those forks.
- **The missing `debug` import** (`c74f8cfc5`).
- Dropped: **the bind span fix** (`b94cb9183`). Upstream's `bind` is `#[instrument(name = "endpoint", fields(id))]`,
  which holds no guard across an await.

## iroh-gossip

- **cdylib crate-type dropped** (`05ef7c0`), as in iroh.
- **iroh, iroh-base and noq by git from drlukeangel's forks** (`80fede4`). Remove with the iroh and
  noq forks.
- **The topic event channel holds 8192 events, not 256** (`9a928f53`). 256 slots is 5-8 ms of a burst, and
  a reader task off the CPU that long loses events to `Lagged` though the subscription's buffer behind
  it is empty. Measured at L1 with 5,000 messages at 25-90k/s: 60-120 lag events per burst at 256, none at
  8192, RDM's receive loop handling every message on every node. Remove when upstream sizes the channel
  or takes its buffer from the subscription.
- Dropped at the move, upstream's v0.102.0 closes the gap:
  - **The net actor never awaits a peer's full send queue** (`637a374`). Upstream's byte-bounded
    per-peer queue (#185) never waits. Its removal condition is met: our
    `reciprocal_send_queue_saturation_never_makes_both_actors_deaf` passes unchanged. The patch's own
    drop was the burst loss of D64: it dropped the Graft and its reply through the same 64-slot queue
    and forgot them, so M11's 5,000 burst delivered 16.5-24.7%. On upstream's queue a dropped broadcast
    is announced again and a control message that does not fit closes the peer; the same run read
    100.0% (D65, Q383).
  - **`peer_topics` pruned on `PeerDisconnected`** (`3ee4d7b`): the tip does it after the topic loop.
- Our regression tests stay beside upstream's and pass on the tip: `peer_disconnected_prunes_peer_topics`,
  `neighbor_down_prunes_lazy_push_queue`, `quit_of_one_topic_keeps_the_connection_other_topics_use`, the
  hyparview metadata prunes, `reciprocal_send_queue_saturation_never_makes_both_actors_deaf`.
- Moved to the queue that replaced theirs: `send_loop_terminates_when_send_channel_closed` is now
  `send_loop_terminates_when_its_queue_lease_is_revoked`; `a_full_peer_send_queue_never_blocks_the_actor`
  now tests data (dropped and counted, the peer stays) and control (closes the peer);
  `a_closed_peer_send_queue_keeps_its_disconnect_cleanup` now tests a send to a peer whose connection ended.
- No test is ignored (Q384, Luke). Upstream ignores three of its own at v0.102.0, each failing when run; at
  its unmerged #239 two of the three still are. Nothing migrated: they test today's code, so:
  - `overflow_during_a_handover_keeps_the_new_connection` goes. `abort` closes both connections on
    purpose, and `aborted_peer_during_a_handover_loses_both_connections` passes for that behaviour.
  - **Known limit, one stream per topic:** `topics_within_the_stream_limit_keep_their_neighbors` and
    `topics_over_the_stream_limit_do_not_join_and_lose_their_neighbors`. Past the peer's limit of
    concurrent uni streams the topics do not all join and the connection closes, again after each
    rejoin. RDM leaves the limit at iroh's 100 (`transport_config`), so a node sharing over 100
    channels with one peer hits it. Rafka's count per peer is not measured.
  - **Known defect, a late `Disconnect`:** `late_disconnect_leaves_a_rejoined_neighbor_apart`. A
    `Disconnect` read after the same peer's new `Join` removes the new neighbour, and the two views
    differ until a later message changes them.
  - Neither is fixed. Only a wire change fixes them, off upstream's wire, and it waits for telemetry
    that shows one biting. These tests state today's behaviour, so a change in it is seen.
- Not carried: the HyParView pending-neighbour patches (R-G4 `61a8cbb`, R-G5 `4b6f558`) were reverted
  (the pin `508fb795`, RDM `bf349ba6`, 2026-10-19).

## RDM

- The pins in the workspace `Cargo.toml` move to the commits above. No RDM source had to change for them.
- With the move, on the same loss (Q383, Q385): `GOSSIP_SEND_QUEUE_BYTES`, 16 MiB per peer where the
  default is 1 MiB (a 300,000 x 256 B flood closed 44 connections at L1 at the default, none at this size),
  and the channel receive queue at 8192 events (`gossip.md` §3.4).
- M11 at L1 and L2 after all of it: 100% of every burst in 3 L1 runs and 1 L2 run, and 95,000 of 95,000 and
  235,000 of 235,000 receipts on every receiver for the 5,000 burst.
