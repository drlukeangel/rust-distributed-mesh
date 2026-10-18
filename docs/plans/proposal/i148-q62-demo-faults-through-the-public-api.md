# Q62 — the demo's faults through the public API: pre-Opened control-stream faults, per-ALPN Direct loss, and the source's held Direct graph

## 1. Spec

- `connections.md` §3.2 (`Direct` vs `Proxy`): "`Direct` rows are shared per `(source, destination)` across every ALPN: one node-to-node reachability fact."
- `connections.md` §4.1 (`Effective route`): "A candidate carrier `C` is eligible when: 1. `C`'s `NodeKind` matches `policy`'s `carrier_kind`, 2. `C != source` and `C != destination`, 3. `source` holds an active `Direct` (`state == Connected`) to `C` whose endpoint incarnations match current membership, and 4. the connections store holds an active `Direct` (`state == Connected`) from `C` to `destination` whose endpoint incarnations match current membership."
- `connections.md` §13 E & `docs/plans/i148-data-plane-connections-prd.md` ("Demo acceptance"): "In `demo/` (pinned consumer, `demo/Cargo.toml`), two `rpc_node`s and one `gateway` exercise the connection API and fault recovery with no direct `iroh` access and no private RDM hook. Every row of the table below is one test cell asserting the connection rows written, the typed errors returned, and the stream bytes delivered ... The three-node trace (`rpc.1` → `gateway.1` → `rpc.2`) is captured into Jaeger and read as a single flame showing the carrier setup, stream forwarding, and reconnect."

## 2. Issue

Two distinct gaps surfaced when staging the 6-row Demo acceptance table in `demo/` using only `rdm_node`'s public `connections` API:

1. **Product defect (`e2` carry-over)**: `ConnectionsHeld` on a source (`rpc.1`) was only populated by `rpc.1`'s own `ConnectionsWriter::record` calls and its startup hydration of `<store_root>/connections/<own>/`. When `gateway.1` established a `Direct Connected` row to `rpc.2` (`<store_root>/connections/mesh1.gateway.1/mesh1.rpc.2--direct.json`), `rpc.1`'s `ConnectionsHeld` never read the peer partitions of `<store_root>/connections/*/*--direct.json`. Earlier `e2` cells worked around this by calling a test helper `seed_carrier_edge` that hand-fed `C -> D` into `source`'s `ConnectionsWriter`. Under the Demo acceptance constraint ("no private RDM hook: only the public API; the demo feeds no row by hand"), `rpc.1` found zero eligible carriers in `discover_carrier`.
2. **Testkit fault expression**: Rows 2, 3, and 4 of the Demo acceptance table require:
   - Row 2 & Row 3: per-`(source, destination, alpn)` Direct dial loss (answering `ConnectError::NotSent` and recording `Direct Failed`) so `A` (`demo/framing/1`) fails Direct 3 times, discovers `gateway.1`, and opens `Proxy`, or restores Direct on `A` while `B` (`demo/bulk/1`) remains blocked on Direct and keeps its `Proxy`.
   - Row 4: the three pre-`Opened` control-stream faults (`ConnectError::RemoteRejected { reason }`, `ConnectError::ProtocolViolation`, and `ConnectError::ControlStreamClosed`) before `Opened` on the carrier/destination's registered ALPN handler, without `demo/` touching `iroh::Endpoint` or registering raw `iroh` protocol handlers.

## 3. Code

- [code] `crates/rdm-node/src/connections_storage.rs`: `FileConnectionsStorage` only read `<root>/<own>/` during hydration (`load_own`), with no method to read fleet-wide active `Direct` indexes `<root>/*/*--direct.json` written by peer nodes sharing the store root.
- [code] `crates/rdm-node/src/connections_writer.rs`: `ConnectionsWriter` held `own: ConnectionsHeld` updated only by `self.storage.write(&row)` on the local node; `Dialer::open` (`crates/rdm-transport/src/connections.rs`) read `self.reg.held` without refreshing peer `Direct` rows from the shared connections store.
- [code] `crates/rdm-node/tests/connection_reconnect.rs`: used a private helper `seed_carrier_edge` to inject `C -> D` `Direct Connected` rows into `source`'s writer instead of letting `source` read `C`'s published `Direct` index from the shared store root.
- [code] `crates/rdm-test/src/gossip_faults.rs`: `i147 e5` established the pattern for qualification faults in `demo/`: a `#[cfg(feature = "testkit-gossip")]` module in `rdm_test` (`gossip_faults::drop_chunk_once`) driving a narrow testkit-only seam on the runtime object while keeping the consumer test on the public API.

## 4. Ruling (`x`, the lead, 2026-10-18)

1. **Fix the `e2` carrier-discovery defect first (RED first on the row-3 cell with no hand-fed row)**:
   - `ConnectionsStorage::fleet_directs` (`FileConnectionsStorage`) scans `<store_root>/connections/*/*--direct.json` and returns the latest `Direct` row per `(source, destination)` across the shared connections store.
   - `ConnectionsWriter::refresh_fleet_directs` (called via `ConnectionsSink::refresh_fleet_directs` at the start of `Dialer::open` before route resolution and carrier discovery) folds those peer `Direct` rows into `ConnectionsHeld` so `discover_carrier` and `resolve` see every active `C -> D` `Direct Connected` row written by any node on the shared store root.
   - `seed_carrier_edge` is removed from `crates/rdm-node/tests/connection_reconnect.rs`; `gateway.1` simply dials `rpc.2` (writing its own `Direct Connected` row to the shared store root), and `rpc.1` discovers `gateway.1` from the store with zero hand-fed rows (RED `976cc56c`, GREEN `ebf94274`).
2. **Expose the faults as testkit seams in `rdm_test::connection_faults` (`#[cfg(feature = "testkit-gossip")]`), parallel to `rdm_test::gossip_faults`**:
   - `rdm_test::connection_faults::block_direct(source, destination, alpn)` and `unblock_direct(source, destination, alpn)`: per-`(source, destination, alpn)` Direct dial block on `source`'s `ConnectionsRegistry`, answering `ConnectError::NotSent` and recording `Direct Failed` through the normal observer path.
   - `rdm_test::connection_faults::fault_pre_opened_once(destination, alpn, PreOpenedFault::{RemoteRejected { reason }, ProtocolViolation, ControlStreamClosed})`: one-shot pre-`Opened` fault on `destination`'s registered `alpn` handler, firing on the next incoming handshake before `Opened` and then clearing itself.
   - No product hook, and no `iroh` access from `demo/`.
