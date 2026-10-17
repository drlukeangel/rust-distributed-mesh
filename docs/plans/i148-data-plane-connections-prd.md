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
impl Node { pub fn connections(&self, alpn: &[u8]) -> Result<Connections, ConnectError>; }   // a registered ALPN

pub struct Connections;                                        // one ALPN's dials and accepts
impl Connections {
    pub async fn open(&self, destination: &NodeId) -> Result<Connection, ConnectError>;   // §5: Proxy held → Direct → one carrier
    pub async fn accept(&mut self) -> Option<Connection>;                                  // admitted connections only; one owner
    pub fn split(self) -> (Dialer, Acceptor);                                              // once
}
pub struct Connection;                                         // one row of the connections log
impl Connection {
    pub async fn open_bi(&self) -> Result<(SendStream, RecvStream), ConnectError>;
    pub async fn accept_bi(&self) -> Result<(SendStream, RecvStream), ConnectError>;
    pub fn id(&self) -> &ConnectionId;                         // the row's id; what node.connections.get lists
    pub fn peer(&self) -> &Birth;
    pub fn kind(&self) -> ConnectionKind;                      // Direct | Proxy { via }
    pub fn closed(&self) -> impl Future<Output = Closed>;      // by name: peer-left, connections-deleted, carrier-gone, refused
}
pub struct SendStream; pub struct RecvStream;                  // AsyncWrite / AsyncRead; iroh never crosses the surface
pub enum ConnectError { UnregisteredAlpn { alpn }, NoActiveRoute, NotSent { reason }, Refused { reason }, Deleted }
```

## The rules

1. **One endpoint.** RDM binds it (`endpoint::bind_exact`) with its two ALPNs and every `Alpn` the
   app registered at `run`, beside its hydration hook and its declared types (i147 e5). An ALPN not
   registered is `UnregisteredAlpn` on `connections()`. The transport config (MTU, idle, stream
   limits) is RDM's, read from the app's `Alpn` only where the page names a knob; relay mode stays
   disabled.
2. **The accept loop is RDM's.** Node RPC and gossip ALPNs go to their servers; every registered
   `Alpn` goes through the admit step (node-connections-admit; `NoCerts` admits by name) and then to
   that ALPN's `accept`. One owner per `Connections`, or `split` once. A refusal is the `Refused` row,
   the acceptor's fact about one dial.
3. **`open` is §5.** A valid Proxy this node holds is taken without a dial; else one Direct dial;
   when that dial provably sent nothing and the ALPN is `forwardable`, one carrier from the active
   Direct edges of the ALPN's `carriers` to the exact destination, chosen by
   `stable_hash(source, destination, destination_incarnation)` and sticky (§6); else `NoActiveRoute`,
   nothing dialled. The outcome names are route.rs's; nothing here retries or re-routes.
4. **A Proxy is a spliced stream.** The carrier never reads the bytes: it opens its own Direct
   stream to the destination on the same ALPN, admits both legs, and pipes both halves. The source
   writes `Proxy Connected { via }` only on a conclusive end-to-end answer on that stream (§7); an
   Indeterminate end never writes it. Any Direct Connected retires the Proxy (§10). The
   invocation-forward of Node RPC (`forward.rs`) is unchanged and separate: Forward is the control
   plane's word, Proxy the data plane's.
5. **The rows are RDM's.** `Direct Failed` with its recovery round, `Proxy Connected`, `Connected`,
   `Disconnected`, `Refused`: all through the one `ConnectionsWriter`; `node.connections.get` lists
   them, `node.connections.delete { All | [ids] }` closes them by id and `closed()` says so by name.
   Rafka's `ReachHooks` go.
6. **Reconnect is RDM's** (§9): the recovery round, durable backoff, one scheduler per node, one
   attempt at a time, reconstructed from the connections log on restart. `connection_reconnect.rs`
   moves from rafka-node-base to rdm-node; the page's crate name follows.
7. **Rafka keeps the protocol.** `rafka-mesh-v1` frames on the stream, semantic routing
   (`which_broker`), route generations and ACKs, and `rafka-quic-v1`, which is client-facing and
   never node-to-node. Rafka's `Alpn` is `{ "rafka-mesh-v1", forwardable: true, carriers: [gateway] }`,
   today's policy stated once.
8. **Observability.** The existing connection spans (`rdm.node.connections.create.via-admitted`,
   `.reject.via-refused`, `.update.via-presented`, the route and reconnect spans) are emitted by the
   surface; the app's stream traffic is not spanned by RDM. A dial parents to the span the caller is
   in, the same rule as `Topic::publish`.

## Implementation plan

#### e1 — the surface and Direct

- **Goal → gap.** `Alpn` at `run`, `Node::connections`, `Connections::{open, accept, split}`,
  `Connection::{open_bi, accept_bi, id, peer, kind, closed}`, `SendStream`/`RecvStream`; Direct only;
  the accept loop in RDM for a registered ALPN with the admit step in front; every row through the
  `ConnectionsWriter`. The Node RPC and gossip ALPNs untouched.
- **Acceptance cells (RED first).** `an_unregistered_alpn_is_refused_by_name_on_connections`;
  `a_direct_open_writes_connected_and_the_acceptor_sees_the_same_row_id`;
  `an_inbound_connection_on_a_registered_alpn_passes_admit_before_accept`;
  `a_second_accept_owner_is_refused_by_the_type_system_and_split_is_once`;
  `node_connections_delete_by_id_closes_the_stream_and_closed_names_it`;
  `a_dial_parents_to_the_callers_span_and_the_stream_bytes_are_not_spanned`.
- **State.** Not started.

#### e2 — the route and the spliced Proxy

- **Goal → gap.** `open` runs §5 over the held rows; a provably unsent Direct on a forwardable ALPN
  takes one carrier by the §6 hash; the carrier splices; `Proxy Connected { via }` on a conclusive
  answer only; §10 retirement; `NoActiveRoute` dials nothing.
- **Acceptance cells (RED first).** `a_held_proxy_is_taken_without_a_direct_dial`;
  `a_not_sent_direct_on_a_forwardable_alpn_goes_once_through_the_hashed_carrier`;
  `the_carrier_pipes_bytes_it_never_reads_and_admits_both_legs`;
  `an_indeterminate_end_writes_no_proxy_connected`;
  `a_non_forwardable_alpn_never_takes_a_proxy_even_when_one_is_held`;
  `the_carrier_choice_is_sticky_until_a_named_event_and_spread_by_source`;
  `a_direct_connected_retires_the_proxy`. One live flame: source → carrier → destination, the
  three admits and the Proxy row, retrieved and linked here.
- **State.** Not started.

#### e3 — reconnect moves

- **Goal → gap.** `connection_reconnect.rs` to rdm-node, driven by the log: a `Direct Failed` latest
  for a live destination incarnation with no newer `Direct Connected` is an obligation; one scheduler,
  one attempt, durable backoff (§9.3), reconstructed on restart (acceptance C on the page).
- **Acceptance cells (RED first).** the page's §13 A–D as RDM cells; `a_failed_reconnect_backoff_survives_restart`;
  `one_reconnect_attempt_at_a_time_per_node`.
- **Page.** connections.md §9: "Every node runs the same reconnect mechanism internally in
  `rafka-node-base`" → "in rdm-node"; the target module path → `crates/rdm-node/src/connection_reconnect.rs`
  (applied with e3's landing, text approved 2026-10-17).
- **State.** Not started.

#### The Rafka side — rafka-v2 i141 e12

rafka-node-transport deleted; `start_accept_loop`, the `rafka-mesh-v1` dials, `reach-reroute`'s
`effective`/`select_carrier`/`ReachHooks`, `connection_reconnect.rs` and the last direct `iroh`
dependency go; `which_broker`, `route_ack_writes`, `route_acks` and the `rafka-mesh-v1` frames stay.
The i142 `epoch` rename shrinks to what stays. Recorded in rafka-v2 `docs/plans/i141-node-rpc-substrate-prd.md`.

## Verification

- [ ] e1–e3 green through `rebase-gate-land.sh`, each with its RED log kept in the deploy dir.
- [ ] One source → carrier → destination flame linked in e2's row.
- [ ] mesh-audit: every connection row written through the one writer; no `iroh::` type in rdm-node's public surface.
- [ ] i141 e12 done: no rafka-v2 crate names `iroh` or `iroh-gossip`.

## Cross-references

- `docs/architecture/connections.md` §5–§11, §13; `docs/architecture/lifecycles/node-connections-admit.md`
- `docs/plans/i147-gossip-conformance-prd.md` e5 (the sibling)
- `docs/plans/i145-cert-ownership-prd.md` (the belt moves into this accept loop, post-chaos)
- rafka-v2 `docs/architecture/replica-routing.md` (the "which" half, unchanged)
