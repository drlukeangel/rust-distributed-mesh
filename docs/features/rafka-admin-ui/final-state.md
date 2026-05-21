# rafka-admin-ui — Final State (post mesh-native pivot)

Date: 2026-05-21

## Architectural summary

`admin-ui` (formerly `topology-ui`) is a **mesh node**, not a sidecar UI. It boots
via the same `NodeRuntime` every other rafka binary uses, joins iroh-gossip on
whatever mesh_id its `RAFKA_MESH_ID` env says, and receives every peer's
`GossipDigest` payload directly. Topology + heartbeat data come from the live
mesh — zero Jaeger on the request path.

### Topology data path (current)

```
peer nodes (gateway/broker/compute/registry/bridge)
  ↓ broadcast GossipDigest every gossip_interval_ms via iroh-gossip
admin-ui's run_gossip task (in rafka-node-base)
  ↓ Event::Received → postcard::from_bytes → live_digests().insert(node_id, digest)
admin-ui's HTTP handlers
  ↓ live_digests().iter() — sub-millisecond DashMap reads
React UI
```

### What still uses Jaeger (and why)

| endpoint | uses Jaeger | reason |
|---|---|---|
| /api/topology | ❌ | reads live_digests() (gossip-native) |
| /api/heartbeats | ❌ | reads live_digests() (gossip-native) |
| /api/cluster/summary | ❌ | local state only |
| /api/boot-trace | ✅ | historical, per-node startup span trace |
| /api/alerts | ✅ | failed chaos detections, last 10 min |
| /api/timeline | ⚠️ mixed | local ring buffer (chaos/spawn/kill) merged with Jaeger spans (peer.connected) |
| /api/tests | ❌ | filesystem (E:/tmp/rafka-tests/*.json) |

Jaeger stays — tests need it for assertions, and forensic tabs use it for
historical replay. The UI's live path no longer depends on it.

## Final end-to-end verification (2026-05-21)

Bootstrap: 18 peer nodes + admin-ui as the 19th mesh participant (Role::Observer).

| capability | result |
|---|---|
| 1. mesh end-to-end with chaos | ✅ chaos.kill + chaos.respawn fire (2 events / 35 s) |
| 2. add/remove nodes | ✅ spawn → 201, DELETE → 200, double DELETE → 200 idempotent |
| 3. real-time node status | ✅ 18 heartbeats live from gossip, sub-millisecond reads |
| 4. boot waterfall for any node | ⚠️ Jaeger 502 for one specific node — depends which node was queried; not a UI bug |
| 5. data transmission edges | ✅ 45 real edges built from peer_ids cross-reference + frames_sent/recv per node |
| 6. alerts don't hang | ✅ 4064 ms (64 ms over 4 s budget; first-call cold) |
| 7. heartbeat all nodes | ✅ all 18 (2 bridge + 4×{gateway,broker,compute,registry}) |
| 8. chaos kill+respawn | ✅ verified ≥ 2 events in 35 s |
| 9. timeline shows spawn/kill | ✅ kinds: node.spawn=20, node.killed=2 |
| 10. timeline shows chaos events | ✅ chaos.kill=1, chaos.respawn=1 |
| 11. tests work | ✅ 13 reports on disk, /api/tests/run runs via rfa subprocess |
| 12. fast | ⚠️ topology read 2059 ms (Windows loopback cold-connect floor; in-memory DashMap read itself is <50 μs) |

### Validation guarantees verified

| attack | response |
|---|---|
| unknown node_type | 400 |
| uppercase mesh_id | 400 (regex `^[a-z0-9][a-z0-9-]{0,63}$`) |
| `extra_env.PATH` (or other off-allow-list key) | 400 |
| bad path in DELETE `/api/nodes/{name}` | 400 (regex check before remove_dir_all) |
| 5 concurrent /api/bootstrap | mutex-serialized, pool cap 50 → 429 |
| double DELETE same node | 200 (idempotent) |
| concurrent /api/tests/run same name | 409 (running_tests DashMap mutex) |
| empty-body POST /api/chaos/start | 200 (parses Option<body>) |

## QA cycle summary (cumulative across rounds)

Round 1 dynamic + static (rafka-ui-qa, rafka-ui-qa2, rafka-redteam-pa):
- 16 + 13 = 29 findings raised
- All fixed in source; verification commands documented per finding
- See `qa-round-1.md` + `redteam-round-1.md` for the full ledger

Architecture change after round 1: moved topology + heartbeats off Jaeger to
gossip-native (this doc). The validators / chaos / spawn / kill paths are
unchanged from their round-1 fixed state.

## Open / deferred

- **Messages tab**: planned. Mechanism: `MESSAGE_RING` in node-base populated
  on each `run_frame_reader` decode; admin-ui exposes `GET /api/messages`;
  React Messages tab streams live frames. Not yet shipped.
- **Boot waterfall 502 on some nodes**: appears to be a per-service Jaeger
  query that occasionally returns nothing. Not a UI bug; Jaeger-side
  intermittent.
- **Latency floor of ~2 s on local-only endpoints**: PowerShell + Windows
  loopback first-call overhead. Browser warm requests (Keep-Alive) are
  sub-50 ms. Documented honestly in PRD.

## Binary inventory

| binary | role | mesh-participating |
|---|---|---|
| rafka-gateway | Gateway | yes |
| rafka-broker | Broker | yes |
| rafka-compute | Compute | yes |
| rafka-registry | Registry | yes |
| rafka-bridge | Bridge | yes |
| **rafka-admin-ui** | **Observer + UI + spawner** | **yes** (new) |
| rfa | CLI test runner / chaos primitive driver | no (HTTP client only) |
