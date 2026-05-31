# PRD — Forced-relay round (mesh-v2)

**Status:** Open (do NOT start until `00-topology-cache-and-base-ui-prd.md` is complete + verified)
**Branch:** off `mesh-v2` after 00 lands
**Builds on:** the topology cache + two-mesh base UI from 00. Same named nodes
(`mesh1.gateway1`, `mesh2.broker1`), same gossiped cache, same admin-ui.

## 0. What this is

00 proved the cross-mesh write works **directly** (two meshes on one host = loopback-reachable, relay
idle). This round proves the other half: when there is **no direct path**, the **relay actually carries
the cross-mesh write** — and you can see it happen live in the admin-ui.

Nothing about the model gets more complex. Still `mesh1.gateway1 → mesh2.broker1`, still resolved from
the cache. The only change: `mesh2`'s nodes are made reachable **only via the relay**, so the gateway's
connection is forced onto the relay path. No new node types, no partitions, no DHT, no app-layer.

## 1. The mechanism — force relay with iroh's own libraries (NO Docker, NO firewall)

Forcing relay must not fight the OS network (Golden Principle #1 — "Windows firewall behaved
unexpectedly" is a banned bug report). iroh gives us the knob directly, verified in the fork:

- **Relay-only address.** An `EndpointAddr` that carries a relay URL and **no direct transport
  addresses** can only be reached through the relay. Confirmed in the iroh fork:
  `iroh/src/protocol.rs:864` — `EndpointAddr::new(id).with_relay_url(relay_url)` (no direct addrs).
  Also `EndpointAddr::from_parts(...)` with a `TransportAddr`.
- **Local relay server.** The `iroh-relay` binary exists in the fork (`E:\iroh\iroh-relay`, `server.rs`
  + `main.rs`); running a local relay is documented at `iroh/docs/local_relays.md`. Endpoints point at
  it via `RelayMode::Custom(RelayMap)` built from the local relay URL.

**So the forced round = mesh2's nodes advertise a relay-only `location` in the cache** (their relay URL,
no loopback addr). The gateway resolves `mesh2.broker1` from the cache exactly as in 00, gets a
relay-only address, and iroh has no choice but to carry the write over the relay. Delivery therefore
**proves** relay-carriage — there is no direct path to fall back to.

## 2. Toggle (env-var, off by default — Golden Principle #8)

- `RAFKA_FORCE_RELAY` (default `0`/unset): when `1`, a node advertises a **relay-only** `location` in
  its gossiped digest (relay URL, no direct addr) and dials cross-mesh peers via their relay-only
  address. Default keeps 00's direct behavior.
- `RAFKA_RELAY_URL` (default empty → relay disabled): the local relay's URL. When set, endpoints build
  `RelayMode::Custom` from it. Document both in CLAUDE.md as part of close-out.
- The local relay is `iroh-relay` run on the host (see `iroh/docs/local_relays.md`). Stand it up; point
  `RAFKA_RELAY_URL` at it.

## 3. What changes vs 00 (and what does NOT)

- **Cache `location`** for a forced node = its relay URL instead of a loopback addr. The cache shape is
  unchanged (`name → {mesh, type, location}`); only the value differs.
- **Connect path**: the gateway builds a relay-only `EndpointAddr` from the cached relay URL and dials
  it. Same write-sim, same `op_kind`, same frames.
- **Unchanged:** node names, gossip, the cache directory, the admin-ui tabs, CPU/RAM telemetry, the
  whole base UI. This round is a reachability change, not a feature.

## 4. Admin-ui proof — the relay is now ON the path (visible difference from 00)

- **Topology:** the cross-mesh edge for the forced write renders **gateway → relay → broker** (the relay
  is a hop on the path), distinct from 00's direct gateway→broker edge. The relay is drawn as active
  infrastructure (on the path), still **not** a mesh node / not in gossip.
- **Messages:** the write-sim still lands at `mesh2.broker1` (the broker records/echoes it) — proving
  delivery despite no direct path.
- **Relay activity:** surface that the relay is carrying traffic (relay server logs, iroh relay metrics,
  or a simple "relay: active, N frames" indicator) — enough to show it is not idle, in contrast to 00.

## 5. Telemetry

Reuse the locked vocabulary — `rafka.mesh.frame.sent` / `rafka.mesh.frame.received` still fire for the
cross-mesh write. Do **not** invent attribute names. If you want to record direct-vs-relay on a frame,
that is a **new attribute on a locked span** → propose it in the sprint config AND append it to the
CLAUDE.md span table in the SAME commit (Golden Principle #10). Otherwise prove relay-carriage by the
construction itself: `mesh2.broker1` advertises no direct address, so a received frame at it could only
have come via the relay.

## 6. Verification — Playwright per phase + a no-direct-path check

Use the harness at `docs/plans/mesh-v2/verify/` (`screenshot.js <phase>`):

- **`phase4-forced-relay`:** Topology showing the gateway→relay→broker path; Messages showing the
  cross-mesh write landed at `mesh2.broker1`; the relay-active indicator. PNGs into
  `screenshots/phase4-forced-relay/`. Inspect them — the edge MUST visibly route through the relay.
- **No-direct-path assertion (anti-confounding):** confirm `mesh2.broker1`'s advertised `location` is
  relay-only (no loopback addr) — e.g. from `/api/topology-cache`. This is what makes delivery a genuine
  proof of relay-carriage rather than a coincidence.

## 7. Acceptance criteria
- With `RAFKA_FORCE_RELAY=1` + a local `iroh-relay` running, `mesh1.gateway1`'s write-sim reaches
  `mesh2.broker1`, and `mesh2.broker1` advertises a **relay-only** location (no direct addr).
- The admin-ui Topology routes the cross-mesh edge **through the relay**; Messages shows the delivery;
  the relay reads as active (not idle).
- With the toggle off, behavior is identical to 00 (direct). No regression to the base UI.
- No Docker, no firewall manipulation; relay is forced purely via iroh address/relay configuration.

## 8. The POC-3 lesson (do NOT repeat)

The earlier forced-relay attempt was confounded: a broker was killed to "force" relay, which dropped the
gateway's pooled connection and surfaced as "connection lost" — masking whether the relay carried
anything. **Force relay only via address/relay configuration (relay-only `EndpointAddr`), never by
killing nodes or cutting connections.** Keep the same nodes alive; change only reachability.

## 9. Critical files
- `crates/rafka-mesh-transport/src/lib.rs` — `RelayMode::Custom` from `RAFKA_RELAY_URL`; build relay-only
  `EndpointAddr` for forced cross-mesh dials (`EndpointAddr::new(id).with_relay_url(url)`).
- `crates/rafka-node-base/src/lib.rs` — when `RAFKA_FORCE_RELAY=1`, advertise a relay-only `location` in
  the gossiped digest.
- `admin-ui/src/main.rs` + `admin-ui/web/src/` — render the cross-mesh edge through the relay when the
  path is relay-carried; relay-active indicator.
- Local relay: run the `iroh-relay` binary (`iroh/docs/local_relays.md`).

## 10. Out of scope
Multi-relay, relay failover, real cross-host/cross-network deployment, NAT scenarios, any app-layer or
partition/replication concept. This round proves one thing: relay carries the write when direct is
unavailable, visible live.
