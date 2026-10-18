# i148 — the data plane through RDM's `Connections`: one endpoint, RDM owns every ALPN

**Status:** approved 2026-10-17 (Luke: "yes", after the i147 e5 shape). Not started. **Record:** this
file; every state or remaining-work update goes in an epic's row below. **Owner:** the i143 lead;
Codex in lane e7s3, after i147 e5 (the rafka lead stays on i144). **Sibling of i147 e5:** e5 is the
gossip half of "Rafka links iroh only through RDM"; this is the connections half. **Not i145:**
i145 (held post-chaos) moves the member-cert check into an accept loop that must already be RDM's;
this is what makes it RDM's.

## Context

The pages already give RDM the endpoint, the connections log, the Direct/Proxy route projection, the
carrier policy, the reconnect series and the accept loop ([connections.md](../architecture/connections.md)
§5–§10; [node-connections-admit](../architecture/lifecycles/node-connections-admit.md): "inside the
accept loop RDM owns", "every inbound connection whose ALPN the embedding app registered at bind").
The code does not. Rafka today:

- builds the iroh endpoint itself (`rafka-v2/crates/rafka-node-transport/src/iroh_transport.rs`,
  `new_with_index_and_hooks`: MTU, relay mode, three ALPNs) and owns the accept loop
  (`rafka-node-base/src/lib.rs::start_accept_loop`);
- dials `rafka-mesh-v1` with iroh directly and speaks its frames on the bi-streams;
- carries a second copy of §5–§8 in `rafka-reach-reroute` (`effective`, `select_carrier`,
  `ReachHooks`) and of §9 in `rafka-node-base/src/connection_reconnect.rs`, beside the half that is
  Rafka's own (`which_broker`: `orgs.topics`, re-sync rows, the write set, replica-routing.md);
- proxies a data-plane frame by having the carrier read it and re-dial the broker, which works only
  because the carrier understands `rafka-mesh-v1`.

RDM has the Node RPC route (`crates/rdm-transport/src/route.rs`: `Direct | ViaPeer(carrier) |
NoActiveRoute`; `forward.rs`: the carrier's one inner call) and the `ConnectionsWriter`
(`crates/rdm-node/src/connections_writer.rs`), but no surface an app can open a connection through.

## The cut (connections.md §11)

```text
Topic + orgs.topics -> WHICH broker        Rafka: which_broker, route_ack_writes, route_acks, replica-routing.md
connections         -> HOW it is reached   RDM: effective, select_carrier, ReachHooks, connection_reconnect, the endpoint, the accept loop
```

A Direct/Proxy transition never touches a route generation or the #2640 ACK; an `orgs.topics` change
never rewrites a connection row. Unchanged.

## The API (`rdm_transport::connections`, re-exported by rdm-node; names for Luke to object to)

```rust
pub struct Alpn { pub name: Vec<u8>, pub forwardable: bool, pub carriers: Vec<NodeKind> }  // registered at bind
impl Node { pub fn connections(&self, alpn: &[u8]) -> Result<Connections, ConnectError>; }   // once per ALPN; a second call is AlreadyTaken

pub struct Connections;                                        // one ALPN's dials and accepts; not Clone
impl Connections {
    pub async fn open(&self, destination: &Birth) -> Result<Connection, ConnectError>;    // exact birth; §5: Proxy held → Direct → one carrier
    pub async fn accept(&mut self) -> Option<Connection>;                                  // admitted connections only; one owner
    pub fn split(self) -> (Dialer, Acceptor);                                              // once
}
pub struct Connection;                                         // one row of the connections log
impl Connection {
    pub async fn open_bi(&self) -> Result<(SendStream, RecvStream), StreamError>;         // any number, either end
    pub async fn accept_bi(&self) -> Result<(SendStream, RecvStream), StreamError>;
    pub fn id(&self) -> &ConnectionId;                         // the row's id; what node.connections.get lists
    pub fn peer(&self) -> &Birth;
    pub fn alpn(&self) -> &[u8];
    pub fn kind(&self) -> ConnectionKind;                      // Direct | Proxy { via: Birth }
    pub async fn closed(&self) -> Closed;                      // by name: PeerLeft | ConnectionsDeleted | CarrierGone | Refused { reason } | Reset { code }
}
pub struct SendStream;  // AsyncWrite + finish() + reset(code: u32)
pub struct RecvStream;  // AsyncRead  + stop(code: u32)
pub enum StreamError { Reset { code: u32 }, Stopped { code: u32 }, Finished, Closed(Closed) }
pub enum ConnectError { UnregisteredAlpn { alpn }, AlreadyTaken { alpn }, NoActiveRoute, NotSent { reason }, Refused { reason }, Deleted, WrongIncarnation { held, found } }
```

iroh never crosses the surface: `SendStream`/`RecvStream` wrap QUIC's, and the three controls
(finish, reset with a code, stop with a code) and their typed errors are the whole of what an app
protocol can see, so a pre-dispatch refusal (a reset code) stays distinguishable from an uncertain
execution (a closed connection) exactly as Rafka distinguishes them today.

## The rules

1. **One endpoint.** RDM binds it (`endpoint::bind_exact`) with its two ALPNs and every `Alpn` the
   app registered at `run`, beside its hydration hook and its declared types (i147 e5). An ALPN not
   registered is `UnregisteredAlpn`; `connections(alpn)` hands out the one `Connections` once and a
   second call is `AlreadyTaken` by name, so there is one accept owner at runtime as well as at
   compile time. The transport config (MTU, idle, stream limits) is RDM's; relay mode stays disabled.
2. **The accept loop is RDM's.** Node RPC and gossip ALPNs go to their servers; every registered
   `Alpn` goes through the admit step (node-connections-admit; `NoCerts` admits by name) and then to
   that ALPN's `accept`. A refusal is the `Refused` row, the acceptor's fact about one dial.
3. **A row is one transport connection on one ALPN.** `NodeConnection` gains `alpn`; the latest-
   per-kind index becomes `(source, destination, alpn, kind)`. Reachability is per protocol: a
   working ALPN A proves nothing about B, a reconnect on A never retires B's Proxy, and carrier
   candidates are active Direct edges on the same ALPN from the ALPN's `carriers` to the exact
   destination. Durable format moved. (Node RPC's own pool connections are the control plane's and
   are not rows: connections.md §1, "a connection a node originated for traffic".)
4. **`open` is §5, against an exact birth.** A valid Proxy this node holds on that ALPN is taken
   without a dial; else one Direct dial; when that dial provably sent nothing and the ALPN is
   `forwardable`, one carrier by `stable_hash(source, destination, destination_incarnation)` over
   the eligible edges, sticky (§6); else `NoActiveRoute`, nothing dialled. A destination that
   answers as another incarnation is `WrongIncarnation`, never served. The outcome names are
   route.rs's; nothing here retries or re-routes.
5. **A Proxy is an RDM tunnel, and its handshake is the proof.** The source opens one connection to
   the carrier on the same ALPN whose first bi-stream is RDM's control stream (the admit step's
   own pattern): `Tunnel { destination: Birth, alpn }`. The carrier dials the destination Direct on
   that ALPN, admits, and the destination's acceptor answers on the control stream with its exact
   birth and `admitted`; the carrier writes that answer back to the source. That answer is the
   conclusive end-to-end proof §7 requires for a stream: the destination itself, at the exact
   incarnation, reached and admitted through this carrier. Only then is `Proxy Connected { via }`
   written; a refusal, a wrong incarnation or a lapse of one staleness floor writes nothing and
   `open` ends by name. The app never reports anything and no application byte, EOF or admission
   on one leg substitutes. After the handshake the tunnel is a `Connection` like any other: every
   bi-stream the source opens is paired by the carrier with one it opens on the destination leg,
   every bi-stream the destination opens on its leg is paired with one the carrier opens toward
   the source, any number, concurrently, 1:1; finish, reset and stop cross the pair with the same
   code; a leg closing closes the tunnel and every paired stream with `CarrierGone` or `PeerLeft`.
   The carrier never reads an app byte. Forward (`forward.rs`, the control plane's one inner call)
   is unchanged and separate. **One control exchange on the first bi-stream, Direct and tunnel
   alike:** the dialer writes its member cert, the acceptor writes the verdict (the admit step as
   the page states it); after `admitted` the dialer writes its intent on the same stream, `Direct`
   or `Tunnel { origin, destination }`, and the acceptor answers `Opened { peer: Birth }` or
   `Refused { reason }`; for a tunnel the carrier answers only after its own dial to the destination
   has run the same exchange with intent `Tunnel` and the destination, being that exact birth, has
   answered `Opened`. Application streams are admissible on either end only after `Opened`; the
   control stream stays open for the connection's life and its close is the connection's (Luke,
   2026-10-17: the streams stay open; nothing is closed after the exchange, and an application
   stream lives as long as the app keeps it, never per request). On both
   ends of a tunnel `peer()` is the far end (source ↔ destination) and `kind()` is
   `Proxy { via: carrier }`; the carrier is the only party that sees two legs.
6. **Direct restoration retires the route, not the tunnel** (§10). `Proxy Disconnected(direct-
   restored)` lands as §10 says, so the next `open` goes Direct; a tunnel already open and its
   streams run to their own end, and until then it is a held connection: `node.connections.get`
   lists it by its row id with `route: retired`, `node.connections.delete` closes it by that id,
   its end writes no second row and is spanned `rdm.node.connections.delete.via-tunnel-ended`.
7. **The rows are RDM's.** `Direct Failed` with its recovery round, `Proxy Connected`, `Connected`,
   `Disconnected`, `Refused`: all through the one `ConnectionsWriter`; `node.connections.get` lists
   them, `node.connections.delete { All | [ids] }` closes them by id and `closed()` says so by name.
   Rafka's `ReachHooks` go.
8. **Reconnect is RDM's** (§9), per ALPN: the recovery round, durable backoff, one scheduler per
   node, one attempt at a time, reconstructed from the connections log on restart.
   `connection_reconnect.rs` moves from rafka-node-base to rdm-node; the page's crate name follows.
9. **Rafka keeps the protocol.** `rafka-mesh-v1` frames on the stream, its reset and stop codes,
   semantic routing (`which_broker`), route generations and ACKs, and `rafka-quic-v1`, which is
   client-facing and never node-to-node. Rafka's `Alpn` is
   `{ "rafka-mesh-v1", forwardable: true, carriers: [gateway] }`, today's policy stated once.
10. **Observability.** The existing connection spans (`rdm.node.connections.create.via-admitted`,
    `.reject.via-refused`, `.update.via-presented`, the route and reconnect spans) plus
    `rdm.node.connections.create.via-tunnel` (source, carrier, destination, alpn) on the source and
    the carrier; the app's stream bytes are never spanned. A dial parents to the span the caller is
    in, the same rule as `Channel::publish`.

## Arch review, 2026-10-17 (five gaps and one, each closed above)

P1 no Proxy proof → rule 5: RDM's tunnel handshake, the destination's own `admitted` at the exact
birth through the carrier, is the proof; no app confirmation. P1 one splice vs the connection API →
rule 5: a tunnel is a connection; streams pair 1:1 in both directions, concurrently; exact birth on
`open` and on the tunnel; close propagates by name; rule 6 for Direct restoration. P1 stream
controls → finish/reset/stop with codes, `StreamError`, carried through the pair. P1 evidence per
ALPN → rule 3: rows carry `alpn`, the index is `(source, destination, alpn, kind)`, reconnect and
carriers per ALPN. Second pass, same day: the ALPN correction finished on §3, §9.4 and §10;
admission-then-intent as one control exchange (rule 5); a retired tunnel stays held, listed and
closable by id (rule 6, §10). P2 the i141 rows contradicted the cut → fixed in rafka-v2 (e11 drops only
iroh-gossip; e12 drops iroh; the kind set is on JoinNode and `held_hash`, not a digest list).
Accept ownership on repeated `connections(alpn)` → `AlreadyTaken` by name, rule 1.

## Page deltas (connections.md; §2, §3, §3.1, §7, §9.4 and §10 applied 2026-10-17 on Luke's yes; §9 lands with e3)

- node-connections-admit.md, Exchange row, one added sentence (applied 2026-10-17 on Luke's yes: "the streams stay open"):
  "After the verdict the dialer writes its intent on the same stream, `Direct` or
  `Tunnel { origin, destination }` (i148), and the acceptor answers `Opened { peer }` or
  `Refused { reason }`; application streams are admissible only after `Opened`, and the control
  stream stays open for the connection's life."


- §2 `NodeConnection` shape: a field `alpn: Vec<u8>` after `kind`; §3.1 declared index
  `(source, destination, kind)` → `(source, destination, alpn, kind)`.
- §7, after "A post-commit answer timeout ... must not create `Proxy Connected`.": "For a
  stream connection the conclusive answer is RDM's tunnel handshake: the destination's own
  `admitted` at the exact birth, written back through the carrier on the tunnel's control stream.
  No application byte, EOF or one-leg admission substitutes."
- §9 crate name, as e3 states.

## Implementation plan

#### e1 — the surface and Direct

- **Goal → gap.** `Alpn` at `run`, `Node::connections`, `Connections::{open, accept, split}`,
  `Connection::{open_bi, accept_bi, id, peer, kind, closed}`, `SendStream`/`RecvStream`; Direct only;
  the accept loop in RDM for a registered ALPN with the admit step in front; every row through the
  `ConnectionsWriter`. The Node RPC and gossip ALPNs untouched.
- **Acceptance cells (RED first).** `an_unregistered_alpn_is_refused_by_name_on_connections`;
  `a_second_connections_call_for_one_alpn_is_already_taken_by_name`;
  `a_direct_open_writes_connected_with_its_alpn_and_the_acceptor_sees_the_same_row_id`;
  `a_working_alpn_a_proves_nothing_for_alpn_b_and_a_reconnect_on_a_never_retires_bs_proxy`;
  `an_open_against_another_incarnation_is_wrong_incarnation_and_never_served`;
  `finish_reset_and_stop_codes_arrive_typed_on_the_far_end`;
  `an_inbound_connection_on_a_registered_alpn_passes_admit_before_accept`;
  `a_second_accept_owner_is_refused_by_the_type_system_and_split_is_once`;
  `node_connections_delete_by_id_closes_the_stream_and_closed_names_it`;
  `a_dial_parents_to_the_callers_span_and_the_stream_bytes_are_not_spanned`.
- **State.** Landed `f2f4bc8d` (lane `e7s3`, branch `codex/i148-e1-direct-connections`). RED captured at `/home/admin/rust-distributed-mesh-deploy/kept/i148-e1-red.log` (`unresolved import rdm_transport::connections`). Implemented `rdm_transport::connections` (re-exported by `rdm-node`): `Alpn`, `Connections::{open, accept, split}`, `Dialer`, `Acceptor`, `Connection::{open_bi, accept_bi, id, peer, alpn, kind, closed}`, `SendStream`, `RecvStream`, `Closed`, `StreamError`, `ConnectError`; `Node::connections(alpn)` hands out `Connections` once per registered ALPN and refuses unregistered (`UnregisteredAlpn`) or repeated (`AlreadyTaken`) calls by name; RDM's accept loop runs the admission + intent (`Direct` / `Opened { peer, row_id }`) control exchange on the first bi-stream and keeps the control stream open for the connection's lifetime (`rdm.node.connections.create.via-admitted`, `.reject.via-refused`, `.update.via-presented`); `NodeConnection` carries `alpn: Vec<u8>` (`ConnectionState::Refused` added, `(source, destination, alpn, kind)` latest-per-kind index, per-ALPN route/retirement/recovery obligations, durable format moved); `node.connections.delete` closes active data-plane connections by row id (`Closed::ConnectionsDeleted`). All 10 acceptance cells in `crates/rdm-transport/tests/connections.rs` pass 10/10 (0.15 s), `rdm-mesh-audit` passes 117/117, and `rdm-docgen` reference pages (`wire.md`, `statuses.md`, `spans.md`) are regenerated.

#### e2 — the route and the spliced Proxy

- **Goal → gap.** `open` runs §5 over the held rows of that ALPN; a provably unsent Direct on a
  forwardable ALPN takes one carrier by the §6 hash; the tunnel handshake on the control stream;
  `Proxy Connected { via }` on the destination's `admitted` only; streams paired 1:1 both ways;
  controls and close propagated; §10 retirement of the row, not the tunnel; `NoActiveRoute` dials
  nothing.
- **Acceptance cells (RED first).** `a_held_proxy_is_taken_without_a_direct_dial`;
  `a_not_sent_direct_on_a_forwardable_alpn_goes_once_through_the_hashed_carrier`;
  `the_tunnel_handshake_carries_the_destinations_admitted_at_the_exact_birth_and_only_then_writes_proxy_connected`;
  `a_tunnel_refused_wrong_incarnation_or_lapsed_writes_nothing_and_ends_by_name`;
  `the_carrier_pairs_concurrent_streams_one_to_one_in_both_directions_and_reads_no_app_byte`;
  `a_destination_opened_stream_reaches_the_source_through_the_tunnel`;
  `a_reset_code_on_one_leg_is_the_same_code_on_the_other`;
  `a_carrier_leg_closing_closes_every_paired_stream_carrier_gone`;
  `an_open_tunnel_and_its_streams_outlive_direct_restoration_and_the_next_open_goes_direct`;
  `a_non_forwardable_alpn_never_takes_a_proxy_even_when_one_is_held`;
  `the_carrier_choice_is_sticky_until_a_named_event_and_spread_by_source`;
  `a_direct_connected_retires_the_proxy`. One live flame: source → carrier → destination, the
  three admits and the Proxy row, retrieved and linked here.
- **State.** Landed `dd60d1fb` (lane `e7s3`, branch `codex/i148-e2-proxy-tunnel`). RED captured at `/home/admin/rust-distributed-mesh-deploy/kept/i148-e2-red.log` (`11 passed; 11 failed`). Implemented §5 route resolution (`resolve_for_carriers` and `select_carrier_for_carriers` in `rdm-core::connections`) inside `Dialer::open`: held valid `Proxy` dials `open_tunnel` directly without a Direct dial; `Direct` dials `open_direct` and on a provably unsent `NotSent` for a `forwardable` ALPN selects one carrier deterministically by `spread(source, destination, destination_incarnation)` and dials `open_tunnel`; `NoActiveRoute` dials nothing; invalid `Proxy` (`carrier-incarnation-superseded`, `destination-incarnation-superseded`, `carrier-edge-lost`) retires its row via `record_drops`. Implemented the two-leg RDM tunnel handshake on the control stream (`ControlIntent::Tunnel`, `rdm.node.connections.create.via-tunnel` on source and carrier with `traceparent` propagation across both legs) writing `Proxy Connected { via }` only once the destination answers `Opened { peer }` at the exact birth, and `splice_tunnel` pairing concurrent bi-streams 1:1 in both directions while forwarding `finish`, `reset(code)`, `stop(code)` and leg closes (`Closed::CarrierGone` / `Closed::PeerLeft`). Wired §10 retired tunnels (`ConnectionsRegistry::held_open_tunnels`, `rdm.node.connections.delete.via-tunnel-ended`) into `node.connections.get` (`route: retired`) and `node.connections.delete` by id without writing a second `Disconnected` row. Live 3-node flame (`mesh1.rpc.1 → mesh1.gateway.1 → mesh1.rpc.2`, three `rdm.node.connections.create.via-admitted` spans, both `rdm.node.connections.create.via-tunnel` spans, and the `Proxy Connected` row `ajdf1kq0b46e`): [source → carrier → destination flame](http://localhost:16686/trace/ad10a8dfe5c8ccf74f277656a16c8b58) (raw and queried evidence kept in `/home/admin/rust-distributed-mesh-deploy/kept/i148-e2-flame/`). All 22 `connections::` cells pass 22/22 in 0.32 s (`/home/admin/rust-distributed-mesh-deploy/kept/i148-e2-green.log`), `rdm-mesh-audit` passes 117/117, and `rdm-docgen` reference pages (`spans.md`) are regenerated.

#### e3 — reconnect moves

- **Goal → gap.** `connection_reconnect.rs` to rdm-node, driven by the log: a `Direct Failed` latest
  for a live destination incarnation with no newer `Direct Connected` is an obligation; one scheduler,
  one attempt, durable backoff (§9.3), reconstructed on restart (acceptance C on the page).
- **Acceptance cells (RED first).** the page's §13 A–D as RDM cells; `a_failed_reconnect_backoff_survives_restart`;
  `one_reconnect_attempt_at_a_time_per_node`.
- **Page.** connections.md §9: "Every node runs the same reconnect mechanism internally in
  `rafka-node-base`" → "in rdm-node"; the target module path → `crates/rdm-node/src/connection_reconnect.rs`
  (applied with e3's landing, text approved 2026-10-17).
- **State.** Landed `d67e987f` (lane `e7s3`, branch `codex/i148-e3-reconnect-moves`). RED captured at `/home/admin/rust-distributed-mesh-deploy/kept/i148-e3-red.log` (`1 passed; 4 failed`). Implemented `crates/rdm-node/src/connection_reconnect.rs` (re-exported by `rdm-node` and `rdm-admin`): per-node Direct reconnect scheduler and Proxy retirement reconciler (`Reconciler`, `ReconnectObligation`, `AlpnRecoveryDial`, `RecoveryDial`, `RecoveryAttempt`, `obligations`, `run_due_at`, `registry_recovery_dial`, `spawn_alpn_reconciler`, `spawn_reconciler`) reconstructing `(source, destination, alpn)` obligations and `next_due` backoff (`rdm_core::reconnect::next_due`) from the latest indexed `Direct Failed` rows on `ConnectionsWriter::hydrate` without replaying the raw log, coalescing inbound accepts on registered ALPNs (`ConnectionsSink::accepted` / `AcceptTrigger::accepted`) into the single pending item per `(destination, destination_incarnation, alpn)`, enforcing at most one reconnect dial attempt at a time per node (`Reconciler::{in_flight_attempts, max_concurrent_attempts}`), probing registered data-plane ALPNs via `ConnectionsRegistry::probe_direct`, and reusing held active `Proxy` row ids across subsequent consumer opens so cold discovery writes one `Proxy Connected` row while warm opens reuse the held route without a Direct dial or duplicate `Proxy Connected` row. Applied the approved `docs/architecture/connections.md` §9 delta (`rafka-node-base` → `rdm-node`, `crates/rdm-node/src/connection_reconnect.rs`). First gate read (`gates-e7s3-7fbc7d3f.log`): 478/479 jobs green, `rdm-node:main` failed `leadership::node_base_holds_no_comparator` on `.cmp(` in `obligations` (`connection_reconnect.rs:272–273`); replaced `sort_by(.cmp)` with `sort_by_key` and verified `rdm-node:main` 8/8 green. Second gate (`gates-e7s3-d67e987f.log`): 110 s GREEN, landed `d67e987f`. Follow-up PRD-only commit `f7a10eb7` gate (`gates-e7s3-f7a10eb7.log`, overlapped with concurrent unlocked gate `gates-e4s2-9116f625.log` on `e4s2`): `rdm-node:main` and `rdm-transport:main` GREEN; outside timeouts under dual-gate saturation on `rdm-admin:main` (`node_objects` stream quiet, `store_root_boot` 10 s `via-inherited-root`), `fabric_primary:204`, `rdm-test:main` (`hydrate_before_ready:488`, `probe_*` span flush), `build_op` (30 s bootstrap / stream timeout), and `node_stop` (300 s job timeout). All 5 `connection_reconnect::` cells in `crates/rdm-node/tests/connection_reconnect.rs` pass 5/5 in 0.39 s (`/home/admin/rust-distributed-mesh-deploy/kept/i148-e3-green.log`), `rdm-transport` `connections::` passes 22/22, `rdm-admin` `connections_writer::` passes 17/17, `rdm-mesh-audit` passes 117/117, and `rdm-docgen` reference pages (`spans.md`) are regenerated.

#### The Rafka side — rafka-v2 i141 e12

rafka-node-transport deleted; `start_accept_loop`, the `rafka-mesh-v1` dials, `reach-reroute`'s
`effective`/`select_carrier`/`ReachHooks`, `connection_reconnect.rs` and the last direct `iroh`
dependency go; `which_broker`, `route_ack_writes`, `route_acks` and the `rafka-mesh-v1` frames stay.
The i142 `epoch` rename shrinks to what stays. Recorded in rafka-v2 `docs/plans/i141-node-rpc-substrate-prd.md`.

## Demo acceptance (arch, 2026-10-17): proof through RDM's public API

Run in the separately built, pinned demo consumer (`demo/Cargo.toml`), with no direct iroh access
and no private RDM hook. RED evidence retained; the three-node flame accompanies the assertions on
rows, outcomes and payloads and never replaces them.

| hazard | required proof | epic |
|---|---|---|
| false Proxy establishment | carrier admission alone, application bytes, EOF, a wrong birth and a handshake lapse each create **no** `Proxy Connected`; the destination's `Opened` at the exact birth does | e2 |
| application outcome confused with transport success | establish the tunnel, apply a counter mutation, lose its reply: RDM reports no application success and replays nothing; the counter shows one execution | e2 |
| ALPN interference | register A and B between the same births; restore A while B uses its Proxy: B's route and streams are unchanged | e1 (rows), e2 (route) |
| incomplete connection abstraction | concurrent streams from both ends with distinct payloads: pairing, finish, reset and stop codes hold through the carrier | e2 |
| retirement breaks lifecycle control | restore Direct during a long Proxy stream: that stream continues, the next `open` is Direct, `node.connections.delete` by the tunnel's id still closes it | e2 |
| restart resets reconnect | restart the source before its persisted `next_due`: no early attempt; the right ALPN resumes its obligation with no duplicate attempt | e3 |

## Verification

- [x] e1–e3 green through `rebase-gate-land.sh`, each with its RED log kept in the deploy dir.
- [x] One source → carrier → destination flame linked in e2's row.
- [x] mesh-audit: every connection row written through the one writer and carrying `alpn`; no `iroh::` type in rdm-node's public surface.
- [x] connections.md §2, §3.1, §7 and §9 deltas on main with the code that meets them.
- [ ] i141 e12 done: no rafka-v2 crate names `iroh` or `iroh-gossip`.

## Cross-references

- `docs/architecture/connections.md` §5–§11, §13; `docs/architecture/lifecycles/node-connections-admit.md`
- `docs/plans/i147-gossip-conformance-prd.md` e5 (the sibling)
- `docs/plans/i145-cert-ownership-prd.md` (the belt moves into this accept loop, post-chaos)
- rafka-v2 `docs/architecture/replica-routing.md` (the "which" half, unchanged)
