# Node-cache spike — full roadmap

**Repo:** `E:/dev/rafka-topology-spike` (isolated clean room — mesh + cache mechanics with NO product business logic, so failures are the cache's, not migration/SA logic). **Import target:** `rafka-v2` (v2-mesh). The generic engine is the artifact to import; the spike is where it's proven.

**Why isolated:** debugging mesh/cache changes inside the full product cost ~5 hrs chasing a "gossip wedge" that was actually gateway day-0 migration logic stalling the runtime. The spike strips that out → fast feedback, trustworthy findings.

---

## Architecture (settled, advisor-verified)

**Caches are generic and blind to consumers.** A cache = `name + type + channel + durability`. It does NOT know or track who uses it.

- **Engine lives in `rafka-node-base`** (compiled into BOTH node binaries AND admin = *capability*).
- **node-admin ASSIGNS instances to a node at spawn** (it knows the deployment = *activation*). The node runs what it's handed; it does NOT self-select (that would be node-type-driven, rejected).
- **A node maintains its assigned subset; the admin maintains a live copy of EVERY cache** (same engine, applies everything).

**Write models (4) — encoded in the cache name:**
| type | rule | conflict resolution | example |
|---|---|---|---|
| `shared` | any node writes any key | **epoch monotonic** (`resolve_conflict`; wall-clock is unsafe) | `shared-1` |
| `leader` | only the leader writes | trivial (leader assigns) | `leader-1` |
| `key` | each node writes ONLY its own key | none (keys partitioned by writer), dedicated channel | `key-1` |
| `key-gossip` | self-key, rides `main`/membership gossip | none | `topology-key-gossip` (= topology) |

**Channel:** `Main` (topology + future projections — extensible property, not a special case) or `Dedicated(name)`. Real per-cache iroh gossip topics; reuse node-base's proven main/backbone bootstrap-peer + join-peers seeding (empty-seed subscribe = isolated swarm = the thrash trap).

**Birth-hydration:** `HydrationSource::BirthInjection`, async, keyed by `entity_kind` — admin hands a node its caches' snapshots at spawn → node is "born knowing", routes from t=0. Falls back to peer-donor / replay if absent.

**Durability (optional, per-cache):** `Ephemeral` (default — rebuilt from birth + gossip; right for projections like topology) or `Disk` (snapshot to file + reload on boot, reusing `snapshot()/hydrate_from()`). Maps to `Topic`-backed in v2-mesh (disk is only a fast-restart optimization there; here there are no topics so disk IS the durable backing).

**Transport boundary stays:** cache owns channel-identity (from `entity_kind`) + publish-trigger (gated by write-model); the injected `MeshGossip` does the iroh send. Pulling iroh into the cache breaks separation.

**APIs (admin, from its maintained copies):** `/api/caches`, `/api/caches/<name>`, `/api/channels`, `/api/channels/<name>`.

---

## Verification discipline (every deliverable — NO "complete" without all that apply)

1. **Logic** — unit tests (write models, conflict resolution, birth-hydration). *(engine: 7/7 passing)*
2. **Data** — `curl /api/caches/<name>` shows real per-node data (e.g. the shared cache from MULTIPLE publishers).
3. **Telemetry** — Jaeger (up: OTLP 4316 / UI 16686). Spans: `rafka.cache.apply` / `rafka.cache.reject` (attr `reason=not-leader|stale-epoch` — the observable proof of enforcement) / `rafka.cache.publish` / `rafka.cache.receive`. Captured via `docs/plans/mesh-v2/verify/jaeger-shot.js`.
4. **UI** — Playwright `docs/plans/mesh-v2/verify/cache-shot.js` asserts the `data-testid` contract + saves PNGs (also fills blog #10 image spaces). Builder must emit: tabs `Caches`/`Channels`; `cache-matrix`, `cache-row-<name>`, `cache-type-<name>`, `cache-channel-<name>`, `cache-cell-<name>-<nodetype>`(`data-held`); `channels-grid`, `channel-col-<name>`, `channel-events-<name>`, `channel-event`.

---

## Phases

### ✅ DONE (verified)
- **P1 — topology cache:** entity-cache-backed, gossip-filled, `/api/topology/node`, cache-backed UI tab. Verified live (4 nodes) + browser screenshots.
- **P2 — cache in every node:** moved into node-base; **eviction fix** (reconcile vs `live_digests()`, inherits ~30s keep-alive) — found by chaos soak (cache was growing 4→11, diverging); **30-min chaos soak PASSED** (110 kill/respawns, 59 ids cycled, held at 4, converged).
- **P2b — birth-injection:** admin hands snapshot at spawn; staggered-spawn proved the ramp (node N born with N entries, `self_node`-tagged).
- **Blog #10** — "Extending the mesh with node caches" written (both reasons + image spaces); README index → 10 parts.
- **P3 — Certs (trust boundary):** node-admin = CA holding a single shared-root ed25519 key (`ca-secret.json`; shareable across admins for cross-mesh). `cert.rs`: `issue_cert` / `verify_cert` (CA-sig + NodeId-bind + expiry), hex-postcard wire form. Cert rides the membership `GossipDigest` (`cert` field); `run_gossip` verifies every received digest BEFORE `live_digests().insert` — no valid cert ⇒ `continue` ⇒ never enters topology. Admin self-issues its own cert + enforces `RAFKA_CA_PUBKEY` in-process. **4-layer verified:** unit 6/6 (valid/wrong-CA/tampered/node-id-mismatch/expired/roundtrip); API (`/api/topology` = 5 certified, uncertified suppressed after 15s grace); telemetry (Jaeger `rafka.cert.reject reason=bad-signature`, artifact `verify/screenshots/cert/cert-reject-spans.json`); UI (Playwright `cert-shot.js` PASS — graph shows "6 spawned · 5 nodes", uncertified absent; 3 PNGs). `RAFKA_NO_CERT=true` spawn flag is the proof path (strips inherited cert ⇒ clean bad-signature reject; also caught the env-inheritance footgun where a cert-less child would otherwise present the admin's cert → node-id-mismatch).
- **P4 — Multi-mesh:** two separately-launched node-admins, one per mesh (mesh1 @19090, mesh2 @19091), **sharing one root CA** (copy `ca-secret.json` a→b before b boots; `/api/ca` returns byte-identical pubkey on both = shared-root proof). **node-admin is now the SOLE backbone listener** — backbone subscribe/publish gated to `Role::Observer` only (gateways dropped at `lib.rs` ~587); cross-mesh write-sim arrow intentionally retired (repeater's job). Backbone bridge between the two admins is **mDNS on localhost** (`backbone_seed_ids = seed_nodes`, admin boots with empty seeds + `mdns_enable=true`); cross-HOST would need explicit backbone cross-seeds — noted, not needed for spike. **4-layer verified:** API (both admins' `/api/topology` show BOTH meshes — own via gossip, other via backbone, 3+3; `/api/ca` identical); telemetry (time-bounded Jaeger: only admins emit `rafka.mesh.backbone.subscribed` in the sole-listener run, gateways 0); UI (Playwright `multimesh-shot.js` PASS — each console renders mesh1 (gossip+edges) + mesh2 (tagged "backbone"); 2 PNGs + topology JSON + `ca-pubkeys.txt`). Sequenced per advisor: proved 2-admin bridge with old role logic FIRST, then made sole-listener change, then re-verified.
- **P5 — Repeater (cross-mesh trust bridge):** standalone `rafka-repeater` binary (`run_repeater` in node-base) subscribes to BOTH mesh gossip topics and re-broadcasts each ORIGIN digest verbatim onto the other — reading ONLY `mesh_id` (routing + loop-guard), NEVER the cert. **Trust-agnostic by design** (deliberately no `RAFKA_CA_PUBKEY`); the RECEIVER's existing `run_gossip` cert-check (shared root) is the gate. **Cargo decision (deliberate, per advisor): relays MEMBERSHIP digests** — so a bridged mesh is SUPERSEDED off the backbone directory and rendered as full cert-verified members. Loop-guard: re-broadcast only if `digest.mesh_id == arrival-topic-mesh` (a relayed mesh1 digest on mesh2 carries mesh_id=mesh1 ≠ mesh2 ⇒ terminates in one hop; own echo dropped same way). Discovery = mDNS (localhost). **4-layer verified:** API (foreign-mesh nodes flip `source=backbone,peer_count=0` → `source=gossip,live,peer_count=2-4` in BOTH directions once the repeater runs); telemetry (Jaeger `rafka.repeater.relay` 53 spans bidirectional; **`subscribed_extra`=0 on both admins** = the cross-mesh membership came SOLELY from the repeater, not topic subscription — the headline cross-mesh property); UI (Playwright `repeater-shot.js` PASS — both consoles show both meshes as full members with **gold cross-mesh edges**, "backbone" tag gone; 2 PNGs + 2 topology JSONs + `repeater-relay-spans.json`). **Trust-agnostic corroboration:** a `RAFKA_NO_CERT` node in mesh1 is relayed BLINDLY by the repeater (1 relay span) yet REJECTED by mesh2's receiver (`cert.reject bad-signature`) and suppressed from both meshes — trust lives at the receiver, even across the dumb relay.

### 🔄 IN PROGRESS — Generic cache engine (current phase)
1. Engine in node-base (4 write models, `Channel`, `CacheSpec` blind to consumers, birth-hydration).
2. Admin assigns at spawn; node maintains subset; admin maintains all.
3. Real per-cache iroh gossip (reuse main/backbone seeding). **Build gate: prove ONE cache propagates node→admin → verify ONE of EACH type live (key partitions / leader rejects non-leader / shared epoch-resolves / key-gossip rides main) → THEN scale to 9.**
4. **Durability — option (b): wire ONE disk-backed cache + prove it survives a node restart** (kill node → restart → cache reloads from disk before gossip).
5. Span contract emitted (telemetry verification).
6. **UI tabs:** `Caches` (matrix, shared caches span columns) + `Channels` (live streams, publisher visible). Built to the `data-testid` contract.
7. **4-layer verification** before claiming complete; Playwright + Jaeger screenshots captured.
9-cache demo set: `topology-key-gossip`(key-gossip/main/all), `shared-1`(gateway+compute — the multi-node-type one), `shared-2`(compute), `leader-1`(gateway), `leader-2`(compute), `key-1`(gateway), `key-2`(compute), `key-3`(registry), `key-4`(broker).

### ⏭ DEFERRED
_(none — roadmap complete)_

**Dependency spine:** ✅ cert (shared root) → ✅ multi-mesh (2 admins, same root, admin = sole backbone listener) → ✅ repeater (blind relay on that trust). **Hypothesis CONFIRMED:** the repeater is trust-agnostic and "it works as long as the authority is good" — the shared-root cert gate at every receiver rejects uncertified nodes even across the relay.

### Carry-forward for v2-mesh import
The spike has proven the full mesh/topology/cache/cert/multi-mesh/repeater model. Import to `E:/dev/rafka-V2-new-mesh`: generic cache engine → real `CachedEntity` impls; `Disk` durability → `Topic`-backed; cert module + GossipDigest.cert field + run_gossip enforcement port verbatim; multi-mesh sole-backbone-listener role gate; repeater as a deployable binary. Cross-HOST: replace mDNS backbone/repeater discovery with explicit seed addresses.

---

## Open / carry-forward
- node-admin-as-backbone-listener: queued for after the cache-engine agent (lib.rs conflict).
- Import-to-v2-mesh mapping: spike `Disk` durability → `Topic`-backed; generic engine → real `CachedEntity` impls per cache type (node carries the types it's assigned; node-admin carries all).
