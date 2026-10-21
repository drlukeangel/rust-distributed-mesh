# iroh, noq, netwatch and iroh-gossip: the fixes we keep

The four forks RDM pins were merged with their upstreams on 2026-10-21 (Luke: the most up-to-date
versions before the soak). Each merge is the branch `rdm-upstream-2026-10-21` on drlukeangel's fork;
no fork's default branch moved. This page lists every patch we still carry, why, and when it may go;
and every patch the merge dropped because upstream now does the same. Upstream issues or PRs for the
kept patches: none filed yet.

This page is the one ledger of the forks: rafka-v2 keeps none (Luke, 2026-10-21; its
`docs/eng/vendor-divergence.md` is deleted and its `[patch]` comment points here). A change to a fork
lands here in the same commit as the pin that takes it, with the measurement that justified it.

| Fork | Pinned commit | Upstream merged | Our patches kept | Dropped (upstream does it) |
|---|---|---|---|---|
| noq | `bf723891` | n0-computer/noq main `f40e4024` (noq 1.4.0, 110 commits) | 1 | 0 |
| netwatch, portmapper | `5739114c` | n0-computer/net-tools main `80b449c` (0.20.0, 19 commits) | 4 | 1 |
| iroh, iroh-base | `076e1a46` | n0-computer/iroh main `efcb1c3f9` (after v1.4.0, 34 commits) | 13 | 1 |
| iroh-gossip | `4d582435` | n0-computer/iroh-gossip main `c5f44db` (after v0.101.0, 15 commits) | 4 | 5 |

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

- **The net actor never awaits a peer's full send queue** (`637a374`, tests `fa25f6c`, `6711230`,
  `fcf4263`): a full queue loses that one enqueue to that one peer (counted in
  `msgs_dropped_send_queue_full`), and plumtree repairs it. Upstream still awaits the send there;
  its #186 bounds a busy publisher and a slow stream header elsewhere. Remove when an upstream release
  never awaits a peer's send there and our regression tests pass against it.
- **`peer_topics` pruned on `PeerDisconnected`** (`3ee4d7b`).
- **cdylib crate-type dropped** (`05ef7c0`), as in iroh.
- **iroh, iroh-base and noq by git from drlukeangel's forks** (`80fede4`). Remove with the iroh and
  noq forks.
- Not carried: the HyParView pending-neighbour patches (R-G4 `61a8cbb`, R-G5 `4b6f558`) were reverted
  before this merge (the pin `508fb795`, RDM `bf349ba6`, 2026-10-19).
- Our regression tests stay beside upstream's and pass on the merged code: connection and state
  eviction (`790db7f`), quitting one topic keeps a connection other topics use (`0fff6a8`), the
  hyparview metadata prunes, plumtree's `neighbor_down_prunes_lazy_push_queue`, `state`'s
  `peer_disconnected_prunes_peer_topics`.
- Dropped, upstream's lifecycle work (#161, #178, #186, #191, #195) does the same:
  - exit `connection_loop` and reap `PeerState` when a peer is removed (`f5e7be3`);
  - break `SendLoop` when the send channel closes (`55d8fd2`);
  - evict `peer_data` and `alive_disconnect_peers` on discard and timeout (`c99e2cf`), now `forget_peer`;
  - prune `lazy_push_queue` on neighbour down (`6fc9a33`);
  - `DisconnectPeer` only when the peer's last topic is gone (`d4c6a85`);
  - removing the peer on a dial failure and on a closed connection: upstream's
    `dial_failure_keeps_active_peer` requires the peer kept.
- A test defect, not product behaviour: our `reciprocal_send_queue_saturation_never_makes_both_actors_deaf`
  bounds a gossip convergence at 15 s and misses it under a loaded parallel run (a time bound on gossip).

## RDM

- The pins in the workspace `Cargo.toml` move to the four commits above.
- `fabric_builds.rs`'s Build-topic loop gains a catch-all arm: upstream's #198 makes the gossip
  `Event` enum non-exhaustive.
