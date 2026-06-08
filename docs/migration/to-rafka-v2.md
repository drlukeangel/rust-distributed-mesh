# Spike → Product Migration Plan: `rafka-topology-spike` → `rafka-v2`

**Written:** 2026-06-07  
**Scope:** Bring all spike deliverables (cert, multi-mesh, repeater, bootstrap-2mesh, generic cache engine, UI tabs) into `rafka-v2` Phase 5 without violating any locked product decisions.  
**Read alongside:** `E:/dev/rafka-v2/docs/plans/_master-migration-plan/5-plan.md` (authoritative Phase 5 plan). This doc PROPOSES updates; it does NOT touch any product plan file.  
**Status:** PLAN ONLY — no code written here.

---

## 0. Gating decision: cache engine vs locked "nodes don't keep registries"

**This is the discriminating constraint for the entire migration.** Decide before beginning any cache-engine work.

The spike's generic engine has every node maintaining its assigned cache subset over per-cache gossip topics. The product's 5-plan.md locks: "node-admin = SOLE topology authority; regular nodes do NOT keep a node registry."

These appear to conflict. They do NOT — but only if the cache engine is mapped onto the right use-cases:

| Write model | Use-case in product | Who calls `apply`? | Node keeps it? |
|---|---|---|---|
| `KeyGossip` (main gossip) | Topology cache — each node writes its own entry, rides main gossip | Every spawned node | YES — "each node's assigned subset" = its own key |
| `Key` (dedicated channel) | Per-node state caches (e.g., compaction watermarks, rate-limit counters) | Owning node only | YES — key-partitioned by writer |
| `Shared` (epoch LWW) | Compiled-ACL propagation (5d) — admin leader writes, nodes read | Admin leader writes, all nodes receive | Nodes hold the latest ACL snapshot; admin is the SOLE WRITER |
| `Leader` | Future leader-elected caches | Admin leader only | Admin only |

**The constraint "nodes don't keep a node registry" applies to TOPOLOGY AUTHORITY (who knows the full member list), NOT to nodes holding their own cache entries.** A node holding its own `key` entry in a `KeyGossip` cache IS topology gossip — that IS the mechanism. A node holding the admin's `Shared` ACL broadcast IS the read-side of the cache — the admin is still the sole writer.

**Decision row for user (REQUIRED before Step 2):**
- D-CACHE-1: Approve the write-model → product-use-case table above. If any row is wrong, correct it before Step 2.
- D-CACHE-2: Topic-backed durability scope. Proposal: ONLY the compiled-ACL `Shared` cache is durable (backing Rafka topic = org-scoped system topic, leader-admin single-writer). Topology is `Ephemeral` (rebuilt from gossip). CONFIRM or adjust.
- D-CACHE-3: Scope of Step 3 (cache engine chunk). Proposal: port the engine in 5n, wire ONLY topology + compiled-ACL caches. Defer per-node `Key`/`Key` caches until a concrete consuming feature ships.

---

## 1. Locked product decisions (constraints on every mapping)

From `5-plan.md` — these WIN over any spike approach where they differ:

| Decision | Locked to |
|---|---|
| G1 | 97-byte binary `InternalMeshFrame` struct (5a merged `03625cb`). Spike enum REJECTED. |
| Fork-3/G2 | Identity = iroh NodeId; role = node-admin-signed SVID cert `{NodeId, type, expiry}`. HMAC + `RAFKA_MESH_ROOT_SECRET` RETIRED. |
| G3 | ACL = rebuild-and-clear true-delete + live gossip-push + sender-org check. |
| Topology source | node-admin is SOLE topology authority. Regular nodes do NOT keep a node registry. |
| Hand-rolled mesh | DELETED: `MESH_NODES` / `MESH_CONNECTIONS` / quinn-loops / `apply_node_announce` / TTL eviction loop / `RAFKA_NODE_ID` (u32). |
| Critical path | 5a ✅ → 5n → 5b-mesh-delete → 5c-wiring → 5d → chaos |
| Node-admin imported | 5c-import merged `074ec9a` — but it's a STALE snapshot (pre-cert, pre-cache-engine). |

---

## 2. Per-deliverable mapping

### 2a. Cert module (`cert.rs` + `GossipDigest.cert` + `run_gossip` enforcement)

**Spike state:** `crates/rafka-node-base/src/cert.rs` — fully implemented, 6 unit tests passing. Types: `NodeCert { node_id, node_type, expiry_ms }`, `SignedCert { cert, sig }`. `verify_cert` checks CA-sig + NodeId-bind + expiry. Cert rides the `GossipDigest.cert: String` field (hex-postcard encoded). `run_gossip` verifies every received digest before `live_digests().insert`.

**Product state:** `cert.rs` ABSENT from `crates/rafka-node-base/src/`. `GossipDigest` (line 1502 of product `lib.rs`) MISSING the `cert: String` field. `run_gossip` has NO cert verification. `node-admin/src/main.rs` has `ca: Arc<iroh::SecretKey>` in `AppState` (placeholder) but does NOT call `issue_cert`.

**Locked constraint (Fork-3/G2):** cert-based identity is locked. The product's 5-plan also notes cert enforcement should land as part of 5n.

**Divergence seam — MUST SURFACE TO USER (CONFIRMED CONFLICT, not inferred):**

The spike proves cert enforcement via the gossip-digest path: cert field rides `GossipDigest`, verified in `run_gossip` before any peer enters `live_digests()`. The product's 5-plan.md Fork-3 section (line 611-612) explicitly states:

> "Role/type → node-admin-signed SVID cert `{NodeId, type, expiry}`, **presented at mesh-connect**, verified against node-admin's CA pubkey + the iroh-authenticated NodeId."
> "**Role-trust NEVER rides gossip.**"

**RESOLVED (user decision) — both enforcement points ship: defense-in-depth (belt + suspenders).** The two stances are NOT an either/or; they are two layers of the same `{NodeId, role, expiry}` cert enforced at two points:

- **Belt — mesh-connect SVID (product-locked, PRIMARY):** the SVID is presented as the first bi-stream frame at iroh connection open and verified there. Gates **role / actions** on the data-plane connection. This is the product's locked Fork-3/G2 design; the seam is already merged (`15bd1e4`).
- **Suspenders — gossip-digest cert (spike-proven, SECONDARY):** the cert rides `GossipDigest` and is verified in `run_gossip` before a peer enters `live_digests()`. Gates **membership / admission** (uncertified ⇒ invisible) and — critically — is what lets the **repeater** carry trust cross-mesh, since the repeater relays digests, not connections.

A node must pass **both** gates. Same node-admin CA, same cert content, two enforcement points.

**No hair-splitting (explicit user direction):** do NOT rationalize the gossip layer as "not really role-trust." The product plan's `5-plan.md:612` line "**Role-trust NEVER rides gossip**" is **consciously amended** to permit the secondary gossip auth layer. This is a deliberate one-line plan change, owned and accepted — not a semantic reinterpretation.

**Port consequence:** `cert.rs` ports clean (already true). The `GossipDigest.cert` field + `run_gossip` verification **DO port verbatim** as the suspender layer. The mesh-connect SVID belt is NEW work in 5n (the seam exists at `15bd1e4`; wire SVID issue/present/verify there). Both land in 5n.

**What ports verbatim:**
- `crates/rafka-node-base/src/cert.rs` — copy as-is. No adaptation needed.
- `GossipDigest.cert: String` field — add with `#[serde(default)]` (backward compat with pre-cert nodes during rollout).
- `run_gossip` cert verification block — copy from spike, injecting `RAFKA_CA_PUBKEY` env var.
- 6 unit tests — copy verbatim into the new `cert.rs`.

**What adapts:**
- `node-admin/src/main.rs` spawn path: call `issue_cert(ca, node_id, node_type, ttl_secs, now_ms)` and pass hex cert as env var `RAFKA_NODE_CERT` to spawned process. The product's AppState already has `ca: Arc<iroh::SecretKey>` — wire it.
- Add `handle_ca` handler + `/api/ca` route to node-admin (returns `{ ca_pubkey }` for cross-admin shared-root verification).
- `RAFKA_NO_CERT=true` escape hatch for local dev / unit tests (spike pattern, copy it).

**Plan update needed:** Add "cert module port" as a named step in `5-plan.md`'s 5n chunk. The plan mentions cert identity locked but doesn't name the files or the gossip-digest enforcement path.

---

### 2b. Multi-mesh backbone role gate

**Spike state:** `run_gossip` in `lib.rs` line ~587 has a role gate: only `Role::Observer` (= admin-ui / node-admin) subscribes to the backbone gossip topic. Gateways receive backbone events but do NOT publish onto the backbone swarm. Verified 4-layer: two admins, shared root CA, each admin shows both meshes in `/api/topology`.

**Product state:** `rafka-v2/crates/rafka-node-base/src/lib.rs` — backbone machinery IS present (imported via earlier migration). The role gate (sole-admin-listener) needs to be confirmed as present or added. The product's 5-plan names "node-admin = sole backbone listener" as part of 5n scope.

**What ports:** Confirm the role gate at the backbone subscribe call matches the spike's pattern. If the product's current backbone subscribe is unconstrained (all roles), apply the role guard from spike's `lib.rs:587`.

**Adaptation:** None structurally. The product's role enum and backbone subscribe point exist; the gate is a 2-line conditional. Verify with `cargo check -p rafka-node-base` after adding.

**Plan update needed:** None — 5-plan already scopes this in 5n. Confirm it's accounted for in the 5n implementation task.

---

### 2c. Generic node-cache engine (`node_cache.rs`)

**Spike state:** `crates/rafka-node-base/src/node_cache.rs` — fully implemented. 7 unit tests passing. Key types: `CacheType { Shared, Leader, Key, KeyGossip }`, `CacheSpec { name, cache_type, channel, durable }`, `CacheEntry { value, epoch, publisher, updated_ms }`, `NodeCache`. Process-global statics: `NODE_CACHES`, `CHANNEL_EVENTS`. Async tasks: `run_cache_task`, `run_keygossip_fill`. Disk durability via postcard to `RAFKA_DATA_DIR/cache-<name>.postcard`.

**Product state:** `node_cache.rs` ABSENT from `crates/rafka-node-base/src/`. No cache-engine types exist in the product. `node-admin/src/main.rs` does NOT import `node_caches` or `channel_events`. 5-plan does NOT name a generic cache engine or the 4-write-model type system.

**This deliverable has NO home in the current product plan.** It requires a NEW chunk in 5-plan (proposed: `5n-caches`), gated on 5n (cert + backbone role gate) completing first.

**What ports (with adaptations listed below):**
- `node_cache.rs` — copy as structural basis. Adaptations required (see below).
- Unit tests (7) — copy verbatim.
- `run_cache_task` / `run_keygossip_fill` — copy, wire into `run_node` in product.
- `NODE_CACHES` / `CHANNEL_EVENTS` statics — copy into product's `lib.rs`.
- `parse_cache_specs_from_env()` — copy; env var `RAFKA_NODE_CACHES`.
- `node-admin` cache API: `/api/caches`, `/api/caches/<name>`, `/api/channels`, `/api/channels/<name>` — copy handlers from spike's `admin-ui/src/main.rs`.
- `node-admin` spawn path: assign `CacheSpec` list per node type + pass snapshot (birth-hydration).

**Disk → Topic-backed adaptation (the key difference):**

The spike uses disk (`RAFKA_DATA_DIR/cache-<name>.postcard`) because no Rafka topics exist in the spike. In the product, durable caches should use a Rafka topic as backing store.

Proposed mapping:
- `Ephemeral` caches: behavior unchanged — rebuilt from birth-injection + gossip. No disk, no topic.
- `Durable` caches (initially only compiled-ACL `Shared` cache in 5d scope): replace `persist_to_disk` / `load_from_disk` with a topic append/read pair. The topic is an org-scoped system topic (`$rafka.sys.acl-cache.<org_id>`), single-writer = admin leader. Cold-restart rehydration reads from topic tail.

**Implementation note for Topic-backed:** The `persist_to_disk` / `load_from_disk` interface is already isolated behind a `durable` flag on `CacheSpec`. The adaptation is a new `DurabilityBackend` trait with `Disk` and `Topic` variants, or simply a feature-flag swap. Do NOT collapse disk and topic into the same impl path.

Until Topic-backed is implemented (5d scope), the disk path is acceptable for development/testing. Ship `Ephemeral` topology + disk-backed ACL cache in 5n-caches; swap disk → topic in 5d.

**Real CachedEntity impls (what "generic engine → real impls" means):**

The engine is named-and-typed by `CacheSpec`. The product's concrete instances:
| Cache name | CacheType | Channel | Durable | Owner / writer |
|---|---|---|---|---|
| `topology-key-gossip` | `KeyGossip` | `Main` | `Ephemeral` | Each node writes its own entry |
| `acl-shared` | `Shared` | `Dedicated("acl")` | `Durable` (→ topic in 5d) | Admin leader only |
| (future) `rate-limit-key` | `Key` | `Dedicated("rate")` | `Ephemeral` | Each node writes its own key |

Start with `topology-key-gossip` (already the product's topology model — this is a thin refactor) and `acl-shared` (needed for 5d). Defer `Key` caches until a consuming feature asks for them.

**Span contract (from spike, carry forward verbatim):** `rafka.cache.apply`, `rafka.cache.reject` (attrs: `cache_name`, `reason=stale-epoch|not-leader|bad-writer`), `rafka.cache.publish`, `rafka.cache.receive`, `rafka.cache.hydrate.from-disk`. Add `rafka.cache.hydrate.from-topic` for the Topic-backed path.

**Plan update needed:** Add NEW chunk `5n-caches` to `5-plan.md`, gated after 5n. Scoped to: port engine, wire topology-key-gossip + acl-shared (disk durability first), node-admin spawn assigns specs + birth-hydration, node-admin cache API routes.

---

### 2d. Repeater binary

**Spike state:** `repeater/` crate — thin `main.rs` that reads `RAFKA_REPEATER_MESHES`, calls `rafka_node_base::run_repeater`. Trust-agnostic (no `RAFKA_CA_PUBKEY`). Loop guard via `digest.mesh_id != arrival-topic-mesh`. 4-layer verified: telemetry shows `rafka.repeater.relay` spans bidirectional; foreign-mesh nodes flip from `source=backbone` to `source=gossip,live` once repeater runs.

**Product state:** No repeater crate exists in `rafka-v2`. `run_repeater` may or may not be present in product's `rafka-node-base` — verify with grep before implementing.

**What ports:**
- `run_repeater` function in `rafka-node-base/src/lib.rs` — copy from spike if absent.
- New crate `rafka-repeater` in product workspace (`crates/rafka-repeater/` or `repeater/`). Workspace Cargo.toml entry required.
- `main.rs`: copy from spike verbatim. Reads `RAFKA_REPEATER_MESHES` + `RAFKA_NODE_BIND_ADDR`; initializes telemetry under `OTEL_SERVICE_NAME = "repeater"`.

**mDNS → explicit seeds adaptation:**

The spike uses mDNS for backbone and repeater discovery (works on localhost). The product requires cross-HOST deployment. Replace mDNS with explicit seed address env vars:
- `RAFKA_BACKBONE_SEEDS` (comma-separated `NodeId@addr:port`) — for admin backbone bootstrap.
- `RAFKA_REPEATER_SEEDS` (comma-separated `NodeId@addr:port`) — for repeater to find both mesh admins.

This is the only structural adaptation for cross-host support. The repeater binary itself is otherwise a verbatim copy.

**OQ-1 RESOLVED (user decision):** the repeater is a **wanted capability — ship it.** It's the cross-mesh trust bridge whose whole point is the gossip-digest suspender layer (§2a): it relays digests, so trust travels with the digest and the far-mesh receiver cert-verifies against the shared root. Ships in Phase 5 alongside bootstrap-2mesh and the dual-auth cert model. Cross-host adaptation (mDNS → explicit seeds, below) is required before multi-host tests.

**Plan update needed:** Add new chunk `5-repeater` to 5-plan, after 5n.

---

### 2e. Bootstrap-2mesh (`handle_bootstrap_2mesh`)

**Spike state:** `admin-ui/src/main.rs` `handle_bootstrap_2mesh` — spawns 2 admins (auto-launch 2nd with shared CA) + 2-of-each-type per mesh = 18 nodes total; idempotent cold-start. Route: `POST /api/bootstrap-2mesh`. Span: `rafka.ui.bootstrap_2mesh`.

**Product state:** `node-admin/src/main.rs` (imported 5c-import, `074ec9a`) — the import was BEFORE bootstrap-2mesh landed in the spike. Confirm with grep whether `handle_bootstrap_2mesh` and `/api/bootstrap-2mesh` are present. If absent, they need to be added as part of 5n or 5n-caches.

**What ports:**
- `handle_bootstrap_2mesh` handler — copy from spike's `admin-ui/src/main.rs`.
- Route registration: `POST /api/bootstrap-2mesh`.
- Span emission: `rafka.ui.bootstrap_2mesh`.

**Adaptations:**
- Replace hardcoded mesh counts with product-appropriate defaults or env vars.
- Replace mDNS admin discovery with explicit seed addresses (same as repeater adaptation above).
- Idempotency check: spike checks whether 2nd admin is already running. Adapt to product process management model.

**Scope question for user (OQ-2):** Does Phase 5 ship the 2-admin multi-mesh topology as a supported configuration, or is bootstrap-2mesh a spike convenience that stays in the spike? If Phase 5 is single-mesh, `handle_bootstrap_2mesh` can be deferred.

**Plan update needed:** Confirm presence in product's node-admin. If absent, add to 5n task list. If deferred, note in 5-plan.

---

### 2f. UI tabs: Caches + Channels

**Spike state:** React frontend in `admin-ui/` has two tabs added during the cache-engine phase: `Caches` (matrix view: cache rows × node columns) and `Channels` (live gossip event stream). `data-testid` contract: `cache-matrix`, `cache-row-<name>`, `cache-type-<name>`, `cache-channel-<name>`, `cache-cell-<name>-<nodetype>` (with `data-held`); `channels-grid`, `channel-col-<name>`, `channel-events-<name>`, `channel-event`. Playwright verify scripts: `cache-shot.js`, (implied `channel-shot.js`).

**Product state:** `node-admin/` (imported 5c-import) has the base node-admin React frontend. Whether Caches/Channels tabs are present depends on how complete the 5c-import was. Verify.

**What ports:**
- React tab components for Caches and Channels — port from spike's `admin-ui/src/` React source.
- API consumers hitting `/api/caches` and `/api/channels` (node-admin backend, from 2c above).
- Playwright verify scripts from `docs/plans/mesh-v2/verify/` in the spike — move to `tests/e2e/` or `node-admin/verify/` in product.

**Adaptation:** Product's node-admin frontend may have a different component structure. Integrate tabs into the existing layout rather than wholesale-replacing the frontend.

**Plan update needed:** Add UI tab porting to the `5n-caches` chunk.

---

## 3. Sequenced order of operations

Steps are ordered to respect locked critical path: 5a ✅ → 5n → 5b-mesh-delete → 5c-wiring → 5d → chaos.

```
STEP 1: Cert module port (into 5n)
  Files: crates/rafka-node-base/src/cert.rs (NEW, verbatim from spike)
         crates/rafka-node-base/src/lib.rs  (add GossipDigest.cert field; add run_gossip verification)
         node-admin/src/main.rs            (wire issue_cert in spawn path; add /api/ca route)
  Gate: cargo check -p rafka-node-base; 6 unit tests pass.
  Span: rafka.cert.reject (reason=bad-signature|node-id-mismatch|expired).
  Plan: Note in 5n task — "add cert.rs port + gossip-digest enforcement" as first named sub-step.

STEP 2: Backbone role gate confirm/apply (into 5n)
  Files: crates/rafka-node-base/src/lib.rs (confirm/add Role::Observer gate on backbone subscribe)
  Gate: cargo check -p rafka-node-base.
  Plan: Confirm 5n already scopes this; no new chunk needed.

STEP 3: node-cache engine port (NEW chunk: 5n-caches, after 5n)
  Gate on: STEP 1 complete + D-CACHE-1/D-CACHE-2/D-CACHE-3 decisions resolved.
  Files: crates/rafka-node-base/src/node_cache.rs (NEW, verbatim + disk→topic adaptation hook)
         crates/rafka-node-base/src/lib.rs        (add NODE_CACHES/CHANNEL_EVENTS statics; wire run_cache_task/run_keygossip_fill into run_node)
         node-admin/src/main.rs                   (import node_caches/channel_events; spawn assigns CacheSpec list; birth-hydration snapshot pass; add /api/caches, /api/channels routes)
  Gate: cargo check --workspace --tests --no-default-features; 7 cache unit tests pass.
  Span: rafka.cache.apply, rafka.cache.reject, rafka.cache.publish, rafka.cache.receive.
  Plan: Add NEW chunk 5n-caches to 5-plan.md.

STEP 4: bootstrap-2mesh + /api/ca (into 5n or 5n-caches, pending OQ-2)
  Gate on: STEP 1, STEP 2. Requires mDNS→seed-addr adaptation if cross-host.
  Files: node-admin/src/main.rs (add handle_bootstrap_2mesh; add /api/bootstrap-2mesh if absent)
  Gate: cargo check -p node-admin.
  Plan: Confirm/add in 5n task. Flag OQ-2 first.

STEP 5: Repeater binary (NEW chunk: 5-repeater, after STEP 2, pending OQ-1)
  Gate on: STEP 2 (sole-backbone-listener confirmed). Requires mDNS→seed-addr adaptation.
  Files: crates/rafka-repeater/src/main.rs (NEW, verbatim from spike)
         Cargo.toml workspace member entry (NEW)
         crates/rafka-node-base/src/lib.rs (confirm run_repeater present; add if absent)
  Gate: cargo check -p rafka-repeater.
  Span: rafka.repeater.relay (bidirectional, verified).
  Plan: Add NEW chunk 5-repeater. Flag OQ-1 first.

STEP 6: UI tabs: Caches + Channels (into 5n-caches, after STEP 3)
  Gate on: STEP 3 API routes live.
  Files: node-admin/src/ React components for Caches + Channels tabs
         Playwright verify scripts (from spike's docs/plans/mesh-v2/verify/)
  Gate: Playwright cache-shot.js + channel-shot.js pass; data-testid contract intact.
  Plan: Add UI work to 5n-caches chunk.

STEP 7: 5b-mesh-delete (already in 5-plan, unblocked by STEPS 1-2)
  (product plan work — no spike deliverable maps here; this is product-side cleanup)
  STEPS 1-2 are prerequisites: cert-based topology must be live before deleting hand-rolled mesh.

STEP 8: Topic-backed durability (into 5d, after 5b-mesh-delete)
  Gate on: STEP 3 (disk-backed ACL cache live); Rafka system topics available.
  Files: crates/rafka-node-base/src/node_cache.rs (swap disk → topic for Durable caches)
  Gate: ACL cache survives cold restart via topic replay.
  Span: rafka.cache.hydrate.from-topic.
  Plan: Add topic-backed durability as a named sub-step in 5d.
```

---

## 4. What ports verbatim vs what adapts

| Artifact | Port verbatim | Adapts |
|---|---|---|
| `cert.rs` | Entire file + 6 tests | None |
| `GossipDigest.cert` field | Field + `#[serde(default)]` | None |
| `run_gossip` cert check | Verification block | Env var name may differ |
| Backbone role gate | Conditional at subscribe call | Confirm against product's Role enum |
| `node_cache.rs` | Engine logic + 7 tests + span names | Disk → Topic for Durable path |
| `run_cache_task` / `run_keygossip_fill` | Copy | Wire into product's `run_node` |
| NODE_CACHES / CHANNEL_EVENTS statics | Copy | Add to product lib.rs |
| `parse_cache_specs_from_env` | Copy | |
| Cache API handlers | Copy from admin-ui | Route prefix may differ |
| `handle_bootstrap_2mesh` | Copy structure | mDNS → explicit seeds; process model |
| `/api/ca` handler | Copy verbatim | |
| Repeater `main.rs` | Copy verbatim | mDNS → explicit seeds env var |
| `run_repeater` in lib.rs | Copy if absent | |
| React Caches/Channels tabs | Copy component logic | Integrate into product nav layout |
| Playwright verify scripts | Copy | Update paths for product directory layout |

---

## 5. Seams and risks

### RISK 1 (RESOLVED): Gossip-digest cert + mesh-connect SVID — both ship (defense-in-depth)

~~HIGH conflict~~ → **Resolved by user decision (OQ-4): ship BOTH enforcement points.** The gossip-digest cert (spike, suspenders/membership) and the mesh-connect SVID (product, belt/role) are two layers of the same cert, not an either/or. See §2a. The product plan's `5-plan.md:612` "Role-trust NEVER rides gossip" is **consciously amended** to permit the secondary gossip auth layer — no semantic dodge.

Residual (now MEDIUM, not blocking): the **mesh-connect SVID belt is new work** in 5n (the seam exists at `15bd1e4`, but issue/present/verify over the bi-stream isn't built). The gossip-digest suspender ports verbatim from the spike. Both land in 5n; sequence the belt as new implementation, the suspender as a port.

### RISK 2 (HIGH): Cache engine conflicts with "nodes don't keep registries" misread

If the D-CACHE-1 table (Section 0) is not reviewed before Step 3, an implementer could incorrectly strip the per-node cache-subset model and centralise everything in admin — losing the gossip propagation model that makes the topology cache work. The distinction: nodes keep THEIR OWN ENTRIES (key-partitioned), not a full registry of all nodes.

**Mitigation:** Resolve D-CACHE-1 before beginning Step 3. The cache engine ports conceptually clean; only the use-case mapping needs explicit sign-off.

### RISK 3 (MEDIUM): Stale node-admin import

Product's `node-admin/src/main.rs` (imported `074ec9a`) predates cert, cache-engine, and bootstrap-2mesh in the spike. The migration is a DELTA: cert + cache API + bootstrap-2mesh handlers + `/api/ca` must be added individually. If an implementer does a wholesale re-copy from the spike's `admin-ui/src/main.rs`, they will import spike-specific arch (mDNS, spike node types, spike env var names) that conflicts with the product.

**Mitigation:** Apply each handler as a named delta. Never do a wholesale file-copy of admin-ui → node-admin.

### RISK 4 (MEDIUM): Disk durability ships first, topic-backed deferred

The cache engine will ship with disk durability for the ACL `Shared` cache in Steps 3-6. The topic-backed swap is Step 8 (after 5b-mesh-delete). During this window, the disk path is LIVE in production builds. Ensure the disk path is gated behind an env var or feature flag so it cannot silently run in environments where a topic is expected.

**Mitigation:** Add `RAFKA_CACHE_DURABILITY_BACKEND=disk|topic` env var to the durability branch at Step 3. Default to `disk` initially; switch default to `topic` at Step 8.

### RISK 5 (MEDIUM): mDNS → explicit seeds not implemented before multi-host testing

The spike's entire multi-mesh + repeater proof was mDNS-based (localhost only). Any product test on two physical hosts (or two containers with separate network namespaces) will fail silently if the mDNS dependency is not replaced before those tests run.

**Mitigation:** At Step 4 (bootstrap-2mesh) and Step 5 (repeater), add explicit seed address support as a blocking prerequisite. Do NOT run multi-host smoke tests before verifying seed-address bootstrap works.

### RISK 6 (LOW): Competing eviction loops

5-plan.md flags a potential conflict between `rafka-topology::spawn_eviction_loop` and the grafted node-base eviction loop. The cache engine (Step 3) introduces a third potential eviction site (cache TTL or gossip-keep-alive gating). All three must reconcile to the same `live_digests()` state.

**Mitigation:** When adding `run_cache_task`, confirm it shares the same `live_digests()` global as `spawn_eviction_loop` and does NOT introduce a separate TTL timer. The spike's solution: cache eviction delegates to `live_digests()` — port this pattern, do not add a cache-specific TTL loop.

---

## 6. What is a plan update vs new code

| Item | Plan update (propose in 5-plan.md) | New code |
|---|---|---|
| Cert module port | Add to 5n task list (already in scope conceptually, needs file-level naming) | `cert.rs`, GossipDigest field, run_gossip block |
| Backbone role gate | Confirm 5n scope covers it | Conditional in lib.rs |
| Cache engine | NEW chunk `5n-caches` after 5n | `node_cache.rs`, statics, run_cache_task wiring |
| Cache API in node-admin | Part of 5n-caches chunk | Handlers + routes |
| bootstrap-2mesh | Add to 5n (if in Phase 5 scope) | Handler + route |
| Repeater binary | NEW chunk `5-repeater` (if in Phase 5 scope) | New crate `rafka-repeater` |
| UI tabs | Add to 5n-caches chunk | React components + Playwright scripts |
| Topic-backed durability | Add sub-step to 5d | Durability backend swap in node_cache.rs |

---

## 7. Open questions for the user

**OQ-1 (SCOPE) — ✅ RESOLVED:** Repeater is a **wanted capability; ships in Phase 5.** Add `5-repeater` after 5n. Its raison d'être is the gossip-digest suspender (it relays digests cross-mesh; the receiver cert-verifies against the shared root). Cross-host needs mDNS → explicit seeds first.

**OQ-2 (SCOPE):** Does Phase 5 ship the 2-admin configuration (`bootstrap-2mesh`) as a supported deployment topology, or is it an internal testing convenience? (Leaning supported, given OQ-1 resolved to ship multi-mesh + repeater.)  
- If supported: port `handle_bootstrap_2mesh` in 5n; adapt mDNS → seeds.  
- If testing-only: keep it in spike; exclude from product.  

**OQ-3 (DESIGN):** Confirm D-CACHE-1/D-CACHE-2/D-CACHE-3 from Section 0 before beginning Step 3.

**OQ-4 (ENFORCEMENT) — ✅ RESOLVED:** **Ship BOTH** (defense-in-depth, belt + suspenders): mesh-connect SVID (role/actions, primary) + gossip-digest cert (membership/admission + repeater trust, secondary). Same cert, two enforcement points. `5-plan.md:612` "Role-trust NEVER rides gossip" is **consciously amended** to permit the gossip auth layer — no hair-splitting. The gossip-digest suspender ports verbatim; the mesh-connect SVID belt is new 5n work on the `15bd1e4` seam.

**OQ-5 (DURABILITY GATE):** At what point does the ACL `Shared` cache swap from disk → topic? Is that within Phase 5 (Step 8 above, after 5b-mesh-delete), or deferred to Phase 6 once system topics are stable?

**OQ-6 (REGISTRY SLUG):** The `admin-ui` feature-registry slug is flagged as MISSING in 5-plan.md (BLOCKER 2 for QA). Before Step 6 (UI tabs), confirm the slug is registered in `docs/features/feature-registry.json` and e2e test files carry the `// @feature: node-admin` (or correct slug) tag.

---

## 8. Reference paths

| What | Spike | Product |
|---|---|---|
| cert.rs | `crates/rafka-node-base/src/cert.rs` | `crates/rafka-node-base/src/cert.rs` (TO ADD) |
| node_cache.rs | `crates/rafka-node-base/src/node_cache.rs` | `crates/rafka-node-base/src/node_cache.rs` (TO ADD) |
| GossipDigest | `crates/rafka-node-base/src/lib.rs` line ~1422 | `crates/rafka-node-base/src/lib.rs` line 1502 (ADD `cert` field) |
| run_gossip cert check | `crates/rafka-node-base/src/lib.rs` (in run_gossip) | `crates/rafka-node-base/src/lib.rs` (TO ADD) |
| Backbone role gate | `crates/rafka-node-base/src/lib.rs` ~587 | `crates/rafka-node-base/src/lib.rs` (confirm/add) |
| node-admin CA key | `admin-ui/src/main.rs` `AppState.ca` | `node-admin/src/main.rs` `AppState.ca` (exists, unwired) |
| handle_bootstrap_2mesh | `admin-ui/src/main.rs` line 2388 | `node-admin/src/main.rs` (confirm/add) |
| handle_ca | `admin-ui/src/main.rs` | `node-admin/src/main.rs` (TO ADD) |
| Repeater main.rs | `repeater/src/main.rs` | `crates/rafka-repeater/src/main.rs` (TO ADD) |
| run_repeater | `crates/rafka-node-base/src/lib.rs` | `crates/rafka-node-base/src/lib.rs` (confirm/add) |
| CACHE-ROADMAP | `CACHE-ROADMAP.md` | Reference only |
| Canonical product plan | — | `docs/plans/_master-migration-plan/5-plan.md` |
| Playwright verify scripts | `docs/plans/mesh-v2/verify/` | `node-admin/verify/` or `tests/e2e/` (TO ADD) |
