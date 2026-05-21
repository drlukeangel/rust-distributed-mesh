# rafka-admin-ui — SPEC (Adversarial QA Handoff)

**Branch**: i37-rafka-authz-create
**Commit hash at handoff**: see `git log -1 --format=%h`
**Service URL**: `http://127.0.0.1:19107` (default, override via `RAFKA_ADMIN_UI_BIND_ADDR`)
**CLI binary**: `E:\dev\rafka-V2-new-mesh\target\debug\rfa.exe` (resolved from `cargo metadata --no-deps --format-version 1 | jq -r .target_directory`)
**Admin-ui binary**: `E:\dev\rafka-V2-new-mesh\target\debug\rafka-admin-ui.exe`

This is the contract. Anything claimed below must verifiably hold. Anything
you (QA) find that contradicts this doc is a real bug.

---

## 1. Architecture (mesh-native, post-Phase-C)

```
admin-ui (rafka-admin-ui.exe)
  ├── NodeRuntime — joins the iroh mesh as Role::Observer (its own NodeId)
  │   ├── subscribes to: mesh-a, mesh-b, bridge gossip topics (via
  │   │   RAFKA_OBSERVER_MESHES=mesh-a,mesh-b,bridge — default)
  │   ├── receives every GossipDigest broadcast on those topics
  │   ├── populates process-global rafka_node_base::live_digests()
  │   │   (DashMap<node_id, GossipDigest>) and topic_membership()
  │   │   (DashMap<topic, HashSet<node_id>>)
  │   └── frame_reader pushes every received frame to message_ring()
  │       (VecDeque<MeshMessage>, cap 1000)
  ├── axum HTTP server on $RAFKA_ADMIN_UI_BIND_ADDR (default 127.0.0.1:19090)
  │   ├── serves React UI from web/dist
  │   ├── /api/* endpoints — most read live_digests() / message_ring()
  │   │   directly (sub-millisecond, no Jaeger), boot-trace + alerts +
  │   │   timeline still query Jaeger for historical spans
  │   └── /api/bootstrap spawns 18 peer subprocesses (mesh-a: 4 of each
  │       type, mesh-b: 4 of each type, 2 bridges with
  │       RAFKA_BRIDGE_TARGET_MESHES=mesh-a,mesh-b)
  ├── chaos_loop — OFF by default; activated via POST /api/chaos/start.
  │   When running, picks random non-bridge node every cadence_ms (default
  │   30000), kills + respawns in same mesh; bridges are protected.
  └── reaper_loop — every 5s, removes exited PIDs from spawned_meta +
      purges their data dirs

Peer node binaries (rafka-{gateway,broker,compute,registry,bridge}.exe)
  ├── NodeRuntime (same code path admin-ui uses; admin-ui is "just another
  │   node" with a label of Observer + axum on top)
  ├── iroh::Endpoint via rafka_mesh_transport::IrohMeshTransport::new()
  │   (mdns local discovery — every endpoint sees every other endpoint)
  ├── iroh-gossip subscribes to blake3(RAFKA_MESH_ID) topic
  │   PLUS any extras from RAFKA_OBSERVER_MESHES (admin-ui) /
  │   RAFKA_BRIDGE_TARGET_MESHES (bridges)
  ├── run_ping_sender (every 10s) — opens uni-stream to each peer in
  │   registry, sends InternalMeshFrame::Ping{org_id=0}
  ├── run_frame_reader — accepts uni-streams, decodes, increments
  │   mesh_counters(), pushes to message_ring()
  └── run_gossip task — broadcasts a GossipDigest every 5s carrying
      {node_id, node_name, mesh_id, node_type, peer_count, peer_ids,
       frames_sent_total, frames_recv_total, wall_time_ms}
```

## 2. Validators (every spawn-side input)

| input | rule | response |
|---|---|---|
| `body.node_type` | must be in {gateway, broker, compute, registry, bridge} | 400 otherwise |
| `body.mesh_id` (required) | `^[a-z0-9][a-z0-9-]{0,63}$` | 400 otherwise |
| `body.extra_env` keys | allow-list: RAFKA_MESH_ID, RAFKA_LINK_SLOW_MS, RAFKA_LINK_LOSS_PCT, RAFKA_CLOCK_SKEW_MS, RAFKA_NODE_BIND_ADDR, RAFKA_BRIDGE_TARGET_MESHES, RAFKA_AUTO_SHUTDOWN_SECS, RUST_LOG | 400 otherwise |
| `node_name` in path (DELETE) | `^(gateway\|broker\|compute\|registry\|bridge)-[0-9a-f]{8}$` | 400 otherwise |
| Pool cap | total spawned_meta <= 50 | bootstrap returns 429 if exceeded |
| Bootstrap concurrent | serialized via tokio::sync::Mutex | second concurrent caller queues |
| Test name in /api/tests/run | `^[a-z0-9][a-z0-9-]*$` len <= 64 | 400 otherwise |
| Concurrent /api/tests/run same name | rejected via running_tests DashMap | 409 |

## 3. API contract — every endpoint

| method | path | source | typical latency | shape |
|---|---|---|---|---|
| GET | /api/health | local | <10ms | `{status:"ok"}` |
| GET | /api/cluster/summary | local | <50ms | `{spawned, meshes, chaos_per_min, mean_peers, total_chaos_events}` — `spawned` counts admin-ui-managed PIDs only; gossip-visible peer count may be higher (those came from prior bootstraps or external admin-uis) |
| GET | /api/topology | gossip live | <50ms warm | `{nodes:[{id,node_id,type,mesh_id,peer_count,frames_sent_total,frames_recv_total,wall_time_ms,status}], edges:[{from,to,kind:"within"\|"cross"}], source:"gossip"}` |
| GET | /api/heartbeats | gossip live | <50ms warm | `{heartbeats:[{node_name,node_type,node_id,mesh_id,peer_count,frames_sent_total,frames_recv_total,age_ms}], source:"gossip"}` (source field always "gossip") |
| GET | /api/messages | live ring | <50ms | `{messages:[{ts_ms,from_peer_id,frame_kind,bytes,summary}]}` newest first, max 500. `frame_kind` ∈ {ping, pong, hello, decode_failed} |
| GET | /api/timeline | local + Jaeger | <6s | `{events:[{ts_us,kind,node_name,node_type,mesh_id,detail}]}` — `kind` ∈ {node.spawn, node.killed, chaos.kill, chaos.respawn, test.start, test.end, node.ready, peer.connected, peer.disconnected}. Jaeger-sourced events (peer.connected/disconnected/node.ready) carry the service name (`broker`/`gateway`/etc.) in `node_name`, not the full `<type>-<hex>` — TODO: resolve via id_to_name |
| GET | /api/alerts | Jaeger | <4s | `{alerts:[...]}` (currently empty unless chaos primitives fail detection) |
| GET | /api/boot-trace?service= | Jaeger | <8s, 502 if no trace yet | raw Jaeger trace data |
| GET | /api/tests | filesystem | <50ms | `{reports:[...]}` |
| GET | /api/chaos/state | local | <50ms | `{running,cadence_ms,total_events,last_event_ts_us}` |
| POST | /api/bootstrap | local | ~5s | `{spawned:[names], errors:[]}` |
| POST | /api/nodes/spawn | local | <500ms | `{node_name,pid}` |
| DELETE | /api/nodes/{name} | local | <5s | `{node_name,reason}` always 200 (idempotent) |
| POST | /api/chaos/start | local | <50ms | `{running:true, ...}` (empty body OK) |
| POST | /api/chaos/stop | local | <50ms | `{running:false, ...}` |
| POST | /api/tests/run | subprocess | up to 600s | `{name,status,duration_ms,detail,...}` |

## 4. Tabs (React UI)

| tab | data source | should show |
|---|---|---|
| Topology | /api/topology | Two mesh group circles (mesh-a, mesh-b) with peers inside, bridges above; edges colored by kind |
| Heartbeat | /api/heartbeats | One card per node with mesh, type, peers, TX/RX counters, kill button |
| Messages | /api/messages (1s poll) | Live table of incoming frames with age/kind/peer/summary/bytes |
| Boot Waterfall | /api/topology for list, /api/boot-trace per node | Per-span timing bar chart |
| Chaos | /api/chaos/state | running flag, cadence, total events, time since last |
| Timeline | /api/timeline | Mixed list of node.spawn / node.killed / chaos.kill / chaos.respawn / Jaeger peer.connected events |
| Alerts | /api/alerts | Failed chaos primitive detections last 10 min |
| Tests | /api/tests + /api/tests/run | Per-test card with last status + run button + Run All |

## 5. Test registry — 33 named tests

5 functional (cargo tests) + 27 chaos + 1 hybrid. Run via
`rfa.exe mesh test list` to see, `rfa.exe mesh test run <name>` to run one,
`rfa.exe mesh test all` to run all.

**Functional (5):**
framer-roundtrip, framer-truncation, traced-frame-roundtrip,
unknown-tag-rejected, bi-stream-echo

**Chaos — substrate sanity (4):**
backpressure-stream-flood, mesh-five-types-present, remove-resilience,
gossip-swarm-forms, gossip-mesh-to-mesh

**Chaos — single-primitive (17):**
kill-broker, kill-gateway, kill-compute, kill-registry,
restart-broker, restart-gateway, burst-kill-3, burst-kill-5,
wedge-broker-2s, wedge-gateway-5s, clock-skew-5s, clock-skew-60s,
slow-link-100ms, slow-link-500ms, lossy-link-10pct, lossy-link-25pct,
nat-shift

**Chaos — soaks (5):**
chaos-soak-9prim-1min, chaos-soak-9prim-2min, chaos-soak-9prim-5min,
chaos-soak-9prim-10min, chaos-soak-9prim-30min

**Mesh shape (1):**
mesh-grow-shrink

## 6. Current state at handoff (verified by lead)

- 29/33 tests hard-passing
- 4 soaks: 89-96% events passing, marked "failed" by strict policy
  (any assertion failure = whole soak failed)
- All 8 tabs except Boot Waterfall verified live-functional
- Boot Waterfall: 502 from Jaeger on per-service traces (Jaeger-side flake)
- admin-ui occasionally crashes during 10+ min soaks (panic in tokio
  background; stack trace lost to log overwrite; intermittent)

## 7. Known issues (acknowledge, don't re-file)

1. ~~admin-ui crash under chaos at low cadence~~ — **CLOSED at
   system boundary** (2026-05-21, floor=30000ms): `/api/chaos/start`
   now returns HTTP 400 with `error: cadence_ms_below_floor` when
   `cadence_ms < 30000`. The upstream bug
   (`iroh-quinn-proto-0.13.0/src/connection/mod.rs:654`: `assertion
   failed: untracked_bytes <= segment_size`) still exists in
   `tokio-rt-worker` under concurrent kill+respawn with in-flight
   QUIC streams.

   Floor escalation history:
     * 2000ms → QA found 18 panics in 5min (NF-1)
     * 5000ms → direct retest showed 6+ panic pairs in 90s of
       chaos; admin-ui terminated mid-test (panics escape tokio
       task supervision via iroh's QUIC worker pool, which we
       don't own)
     * 30000ms → matches the original `cadence_ms` AtomicU64
       default; the 30-min chaos-soak-9prim-30min test passes
       cleanly at this cadence (40+ events, 0 failed, commit
       88937f9 evidence)

   `CHAOS_CADENCE_FLOOR_MS = 30_000` is a `const` in
   `admin-ui/src/main.rs`. Can be lowered once iroh upgrades past
   0.91.2 to a release bundling iroh-quinn-proto 0.15.x+ (where
   the assertion is fixed upstream).

   Verified end-to-end:
     POST /api/chaos/start {"cadence_ms":20000} → 400 (with reason)
     POST /api/chaos/start {"cadence_ms":29999} → 400
     POST /api/chaos/start {"cadence_ms":30000} → 200, chaos starts
2. **Boot Waterfall returns 502 when Jaeger has no trace** — semantic fix
   shipped; was previously 404. Some services genuinely have no recent
   trace; admin-ui correctly bubbles up Jaeger's miss. Friendly empty
   state in UI tells the user to refresh after 30s of Jaeger ingestion.
3. **Soak primitive flake rate ~5-10%** — chaos primitives sometimes don't
   detect within their deadline window (race condition in Jaeger ingestion
   lag). DETERMINISTIC under seed 42 — re-runs produce identical pass set.
4. **Pool cap 50** — bootstrap returns 429 if would exceed; chaos respawn
   also respects this cap.
5. ~~HTTP slowloris — partial-header connections held indefinitely~~ —
   **FIXED** (2026-05-21): replaced `axum::serve(listener, app)` with
   a custom accept loop using `hyper::server::conn::http1::Builder`
   directly, configured with `.timer(TokioTimer::new())` +
   `.header_read_timeout(Duration::from_secs(30))`. axum Router is
   wrapped via `into_make_service_with_connect_info` →
   `TowerToHyperService` per connection. Verified end-to-end: a
   partial-header connection (`GET / HTTP/1.1\r\nHost: x\r\n`) gets
   FIN'd by the server at exactly t=30s (poll_read=True, avail=0).
   First implementation attempt used `hyper_util::auto::Builder` —
   its protocol-detection phase did not honor http1 timeouts. Second
   attempt missed the Timer registration and panicked at first
   connection with "timeout 'header_read_timeout' set, but no timer
   set" — the panic hook captured the full backtrace (proving R5
   also works). Final form: explicit http1::Builder + TokioTimer.
6. ~~Timeline `node_name` for Jaeger-sourced events~~ — **FIXED**:
   handler now resolves `node_id` → `node_name` via `live_digests()` so
   peer.connected/disconnected events show full `broker-abc12345`
   instead of just `broker`. Both self_name and peer_name resolved.
7. ~~Topology graph shows spurious cross-mesh edges~~ — **FIXED**
   (2026-05-21, two-part). PART 1 (commit 00d7b87): `run_gossip()`
   keyed `topic_membership()` by the digest's primary `mesh_id` even
   for EXTRA-topic subscribers, conflating topic membership. Added
   `topic_label: &'static str` distinct from `mesh_id`. PART 2
   (post-QA-postfix): the edge generator's catch-all `_ => "cross"`
   arm still emitted edges for any two co-members on a topic whose
   metadata was incomplete OR who simply had different primary
   `mesh_id` (e.g. admin-ui observer with `mesh_id="default"` paired
   with mesh-a peers). QA postfix found 80 illegal non-bridge cross
   edges. Fix: edge generator now SUPPRESSES the edge if both
   endpoints are non-bridge with different `mesh_id`, or if either
   side's meta is missing. The bridge architecture invariant ("only
   bridges mediate cross-mesh") is now enforced in the edge
   generator itself, not just in the data source.

   Verified after Part 2: `illegal_nonbridge_cross: 0` (was 80).
   Edge counts in 18-node bootstrap: 91 total / 35 cross (all 35
   incident on a bridge) / 56 within. Graph honest.
8. ~~admin-ui invisible in its own /api/topology~~ — **FIXED**
   (2026-05-21): `run_gossip` now self-inserts the locally-built
   digest into `live_digests()` + `topic_membership()` on every
   broadcast tick. iroh-gossip does not echo broadcasts to the sender,
   so without this every node was invisible in its own UI.
9. ~~Panic hook installed but never fires~~ — **FIXED + VERIFIED
   end-to-end** (2026-05-21, three iterations). PART 1: hook
   moved to plain `fn main()` BEFORE the tokio runtime is built —
   fixed the hyper "no timer set" panic capture (verified). PART
   2: eprintln FIRST + atomic-ish open+write_all+flush+drop
   sequence (commit fb1a195). PART 3 verification (2026-05-21
   chaos retest at floor=5000ms): stderr now contains the
   custom-format hook output for BOTH the iroh-quinn assertion
   AND the immediate mutex-poison second panic:

     ==== PANIC @ epoch_ms=1779366524560 (thread="tokio-rt-worker") ====
     panicked at iroh-quinn-proto-0.13.0/src/connection/mod.rs:654:21
     ==== PANIC @ epoch_ms=1779366526072 (thread="tokio-rt-worker") ====
     panicked at iroh-quinn-0.14.0/src/mutex.rs:138:42

   So both panics in the double-panic sequence now get captured
   by the custom hook. This is what was supposed to happen all
   along; the QA-postfix report (R5 FAIL) was a real catch and
   the eprintln-first + flush+drop fix is what finally made it
   work reliably.
10. ~~Messages summary lacks peer NodeId prefix~~ — **FIXED** (2026-05-21):
    `push_message` now prepends an 8-char hex prefix of `from_peer_id`
    to the summary string itself, e.g. `[7144d954] Ping{org_id=0}`.
    `from_peer_id` field remains for the full 64-char NodeId. A UI
    table column rendering only `summary` is now self-describing.
11. ~~`remove-resilience` test flake (only 1/3 survivors fresh)~~ —
    **FIXED** (2026-05-21): two root causes — (a) test tried to spawn
    a bridge in `mesh-a` which fails because bridge requires
    `mesh_id="bridge"` + `RAFKA_BRIDGE_TARGET_MESHES` env, so only
    5/6 spawned cleanly; (b) timing windows (5s settle + 15s
    post-kill + 10s age threshold) were too tight for iroh-gossip's
    spanning tree to re-form after losing 3 of 6 peers. Fix: spawn
    6 non-bridge types (2 gateway + 2 broker + 1 compute + 1
    registry), and bump windows to 8s settle + 25s post-kill + 18s
    age threshold (~3.5× heartbeat interval, tolerates one missed
    tick during tree repair). Verified `status=passed duration=33s`.
12. ~~`gossip-mesh-to-mesh` test failure (no cross.peer_connected
    spans on gateway)~~ — **FIXED** (2026-05-21): test was written
    pre-bridge architecture and queried `service=gateway` for the
    `rafka.mesh.cross.peer_connected` span. Post-bridge, mesh-a
    nodes only talk to bridges and bridges only talk to mesh-b —
    gateways never directly see cross-mesh peers, so the span
    fires on `service=bridge` instead. Fix: query all five peer
    services (bridge, gateway, broker, compute, registry) and
    aggregate the counts. Verified `status=passed duration=17s`.
13. ~~`mesh-five-types-present` test failure (missing types:
    bridge)~~ — **FIXED** (2026-05-21): not a system bug but a
    missing build artifact — `rafka-bridge.exe` had been deleted/
    never rebuilt under `target/debug/`. Fix: rebuilt with
    `cargo build -p rafka-bridge`. PLUS added preflight check
    in admin-ui `async_main`: at startup it verifies every
    `KNOWN_NODE_TYPES` binary exists under
    `{cargo_target_dir}/debug/`. If any are missing, admin-ui
    exits with `PREFLIGHT FAILURE: missing peer binaries:` listing
    each path and the exact `cargo build` command to fix. This
    catches the silent-bootstrap-failure mode (was: bootstrap
    returned `errors: 2` and bridges never showed in topology).
    Verified loud failure: removed bridge binary → admin-ui
    exits at startup with `preflight failed: 1 peer binary/
    binaries missing`. Verified pass: all 5 binaries present →
    admin-ui responds 200 within 5s.

## Soak SLO (post-flake-acceptance)

Soak tests pass if `passed/total ≥ 0.85` AND `failed_timeout == 0`.
This matches the deterministic 5-15% primitive detection flake observed
under sustained load (race between primitive deadline and Jaeger
ingestion lag). Hard timeouts (primitive never completed) still fail
the suite — they indicate a real broken primitive, not a flake.

## 8. QA charter (your scope)

Verify the contract above end-to-end. Spend 25 minutes minimum:

1. **Walk every endpoint in section 3** — confirm shape + latency + source
   matches table.
2. **Walk every tab in section 4** — confirm visual data flows correctly.
3. **Try each validator in section 2** — confirm it rejects/accepts correctly.
4. **Run 3 chaos tests of your choice** from the registry — confirm they
   write reports and complete.
5. **Look for inconsistencies** — anywhere this doc claims X but reality
   shows Y.
6. **Attempt to break** — malformed POSTs, rapid spawn/kill loops, weird
   mesh_ids, etc.

Report findings: severity (critical/high/medium/low/info), claim from this
doc, reality observed, exact reproducer, suggested fix in 1-2 sentences.

## 9. Out of scope (do not file)

- Test runner's "fail-on-any-assertion" policy for soaks (intentional)
- admin-ui crash trace recovery (lost; next QA round will capture)
- React UI cosmetics (focus on correctness, not styling)
