# PRD — Topology cache + base UI (mesh-v2)

**Status:** Open
**Branch:** `mesh-v2` (off the restored working baseline, commit `6572b8f`)
**Builds on:** the working substrate — iroh, hardcoded seed nodes, per-mesh gossip
(topic = `blake3(mesh_id)` → `GossipDigest` → `live_digests()` / `topic_membership()`), the admin-ui
operator console.

## 0. What this is

A cross-mesh topology cache + a base UI that works, on **two meshes running locally on one host**.
Named nodes, a gossiped directory so a node can look up another node's location and write to it. Because
both meshes are on one host, every node is reachable over loopback, so **all writes (intra- and
cross-mesh) are direct in this round.** The relay is stood up to replace the bridge but is **idle** here
(it only carries traffic when there is no direct path — a later round).

One line: **`mesh1.gateway1` resolves `mesh1.broker1` and `mesh2.broker1` from the gossiped cache and
sends each a simulated message — both direct, since everything is local.**

## 1. Node model

- **Node types (unchanged):** `gateway`, `broker`, `compute`, `registry` — PLUS `relay`. The `bridge`
  type is REMOVED (§5); the relay takes its structural place as the cross-mesh transport. Compute +
  registry stay exactly as today (the write-sim only exercises gateway↔broker, but the full set stays).
- **Default create stays the same — 8 nodes per mesh:** the bootstrap spawns **2 each of gateway,
  broker, compute, registry per mesh** (8/mesh, 16 across two meshes), as today. The only change vs the
  current bootstrap: drop the 2 `bridge` nodes; stand up **one relay** (infra, not a per-mesh node). Do
  not otherwise change the fleet.
- **Deterministic names:** `<mesh>.<role><N>` — `mesh1.broker1`, `mesh1.gateway1`, `mesh2.broker1`, …
  Per mesh, per role, the index increments. The admin-ui assigns the name on spawn (`RAFKA_NODE_NAME`).
- A node belongs to exactly one mesh (`RAFKA_MESH_ID` = `mesh1` / `mesh2`).

## 2. The topology cache (the core)

> ⚠️ **The cross-mesh part of this section is SUPERSEDED by `03-cross-mesh-backbone-prd.md` (2026-05-31).**
> The "gateway observes both meshes via `RAFKA_OBSERVER_MESHES`" mechanism below does NOT scale (hundreds
> of gateways/mesh ⇒ firehose, forces the admin-ui to be a special all-mesh subscriber). Cross-mesh
> awareness now rides a **backbone** gossip topic carrying per-mesh *summaries* (directory + aggregate
> metrics), published by each mesh's gateway under a soft lease. The intra-mesh cache below (per-mesh
> `live_digests` directory) is unchanged; only the cross-mesh propagation moves to the backbone.

A **gossiped directory** of `node_name → { mesh, type, location }`, where `location` is the node's
reachable address. It is built from the existing gossip, with two reuse-only moves:

- **Add `location` to the gossiped digest.** Extend `GossipDigest` (or a sibling field) with the node's
  reachable bind address so the directory has somewhere to connect. This is the one new field. Reuse
  `run_gossip` / `live_digests()` / `topic_membership()` — **do not build a second gossip system**
  (Golden Principle #1). `live_digests()` already *is* the directory keyed by node_id; this PRD just
  surfaces it as a name→location cache.
- **Spanning two meshes (the part with no bridge):** per-mesh gossip is `blake3(mesh_id)`
  (`lib.rs:282`), so a `mesh1` node does not see `mesh2` by default. The **gateway** — the node that
  writes cross-mesh — subscribes to BOTH meshes' gossip via the **existing `RAFKA_OBSERVER_MESHES`
  multi-topic-join** (`lib.rs:314–360` — the SAME path the bridge used via `RAFKA_BRIDGE_TARGET_MESHES`)
  and is seeded to one node per mesh. Because `live_digests()` is process-global (`lib.rs:633`), the
  gateway's cache then spans both meshes for free. Brokers stay single-mesh. No bridge node; no shared
  "global" topic — this is reuse, not new gossip.
  - The gateway thereby becomes gossip-present in both meshes (it broadcasts its own digest into each
    subscribed topic, as the bridge did) — that membership is what draws the direct gateway→broker
    cross-mesh edge in Topology.
  - **Keep the R2 guard (`lib.rs:814–820`):** an extra-topic subscriber files received digests under the
    EXTRA mesh's `topic_label`, NOT its own primary `mesh_id`. Skipping this regenerates the O(n²)
    spurious-cross-mesh-pairs bug. Do not regress it.
- **How a node writes:** look up the target name in the cache → get its `location` → `endpoint.connect`
  to that address → send. In this local round every address is loopback-reachable, so this is a
  **direct** connection for both intra- and cross-mesh.

## 3. Write simulation

`mesh1.gateway1` periodically sends a simple "message" frame:
- to `mesh1.broker1` (intra-mesh) and
- to `mesh2.broker1` (cross-mesh),
both resolved from the cache and connected **directly** (local). The receiving broker records/echoes it
so it shows up in the Messages tab. It's a simulation: a node resolves another from the cache and gets a
message to it.

## 4. The relay (replaces the bridge) — present but idle this round

- The relay is the **cross-mesh transport** — a server addressed by URL, **NOT a mesh node** and not a
  gossip participant. It replaces the bridge structurally.
- **In this round it carries no traffic.** Both meshes are local, so direct connectivity always wins;
  the relay sits idle. Stand it up (`iroh-relay --dev` locally, **NO Docker**) so the cross-mesh
  transport *is* the relay going forward, but do not claim cross-mesh writes go through it here — they
  don't. The relay's actual job (carrying when there is no direct path) is proven in a **later
  forced-isolation round**, not this one.
- The admin-ui may show the relay as infrastructure; never as a node in the mesh circles.

## 5. Remove the bridge entirely

- Delete `Role::Bridge`, `RAFKA_BRIDGE_TARGET_MESHES`, the `+ bridge` spawn button, and every place the
  admin-ui spawns or renders `bridge` nodes. Cross-mesh awareness comes from the gateway observing both
  meshes' gossip (§2); cross-mesh transport is the relay (§4) — neither is a bridge node.

## 6. Base UI — what each tab must show (data source in parens)

**Current baseline reality (verified LIVE via Playwright against the running admin-ui, not from
source-grep):** the admin-ui is a **React + react-flow SPA built by Vite**, served from `web/dist`
(`main.rs:4351` `ServeDir`; the inline HTML in `main.rs` is dead legacy — `main.rs:47`). The rendered
tab bar is: **Topology, Nodes, Messages, Boot Waterfall, Chaos, Timeline, Alerts, Tests** — captured in
`verify/screenshots/phase0-baseline/`. So **Messages AND Nodes already exist and are live** (Messages
shows `message_ring()` frames; Nodes shows per-node CPU/RAM cards). **Only the Cache tab is new.** All
UI changes below are **React/TSX edits in `admin-ui/web/src/` + a `npm run build` Vite rebuild into
`web/dist`** — NOT inline HTML in `main.rs`. (See the Golden-Principle-#4 flag in §12.)

| Tab | Status | Endpoint | Source | Must show |
|---|---|---|---|---|
| **Topology** | exists | `/api/topology` | gossip (`live_digests` + `topic_membership`) | each spawned node grouped by mesh, **with its CPU/RAM usage bars**; cross-mesh write as a direct gateway→broker edge; relay (if shown) as idle infra, not a node; **deduped by node_id** |
| **Nodes** | exists | `/api/nodes/spawned` + `/api/heartbeats` | spawn-state + gossip | per-node card: name/type/mesh/peers/age/status + **CPU/RAM usage bars** (`cpu_used`/`cpu_budget`, `ram_used`/`ram_budget`); no `<unspawned>` for live nodes |
| **Messages** | exists | `/api/messages` (`main.rs:3161`) | `message_ring()` | live frames + the write-sim messages. Confirm a freshly-spawned node emits at least one message (a join/boot announce) so this is non-empty for a single node. |
| **Boot Waterfall** | exists | (Jaeger) | OTLP boot spans | the node's `rafka.mesh.boot.*` chain. Requires Jaeger up (localhost:16686). |
| **Timeline** | exists | `/api/timeline` (Jaeger) | OTLP `rafka.mesh.node.ready` + events | the node-creation event when spawned |
| **Cache** | **BUILD tab + endpoint** | `GET /api/topology-cache` (NEW) | the gossip directory (§2) | the `name → {mesh,type,location}` directory; for one node, that node's entry |

> **PRESERVE the CPU/RAM load telemetry.** The per-node CPU/RAM bars come from the substrate's
> `LoadSampler` → the digest's `cpu_used`/`cpu_budget`/`ram_used`/`ram_budget`, plus the alert
> thresholds `RAFKA_CPU_ALERT_THRESHOLD` / `RAFKA_RAM_ALERT_THRESHOLD_GB`. Keep it on the node cards in
> Topology and Nodes, exactly as today.

### 6a. Tab routing — direct per-tab URLs (folded into sprint-12)

The SPA currently switches tabs via React state only; the URL never changes, so a tab cannot be linked
directly. Add routing so each tab is a **direct, clean path URL** (NOT hash): `/topology`, `/nodes`,
`/messages`, `/boot-waterfall`, `/timeline`, `/alerts`, `/chaos`, `/tests`, `/cache`. Visiting the URL
opens that tab on load, refresh stays on it, clicking a tab updates the address bar (`history.pushState`),
back/forward work. **Server:** add an SPA fallback so any GET that isn't `/api/*` or a real static asset
serves `web/dist/index.html` (path-based deep links 404 otherwise); the fallback must NOT shadow `/api/*`.
KISS — a tiny client router suffices. This is what lets the **sprint release docs deep-link to a specific
admin-ui tab** (e.g. `/cache`) instead of just the app root. Acceptance: a direct deep-link (`:port/cache`)
opens that tab and survives refresh; the per-tab links appear in that sprint's release doc.

## 7. Node lifecycle — add / delete

- **Add:** `+ gateway` / `+ broker` / `+ relay` → `POST /api/nodes/spawn` with the chosen mesh → node
  spawns, joins gossip, appears in Topology + Cache within a couple seconds.
- **Delete/kill:** terminates the child AND removes it from `spawned_meta`, the topology, and the cache
  promptly (no lingering ghost / `<unspawned>`).

## 8. Build milestones (prove each LIVE in the admin-ui before the next)

1. **Base UI, ONE mesh ("Step 1"):** admin-ui up with one mesh and **zero** other nodes → click
   **+broker** → `mesh1.broker1` is visible in **Topology, Messages, Boot Waterfall, Timeline, and the
   Cache view**. First acceptance gate.
2. **Two meshes + cross-mesh cache (local):** start `mesh2` locally; the gateway observes both meshes so
   the cache holds both; `mesh1.gateway1` resolves `mesh2.broker1` from the cache and the write-sim
   reaches it **directly** (visible in Messages; direct cross-mesh edge in Topology). Relay stood up but
   idle. No bridges, no duplicates, no `<unspawned>`.
3. **Add/delete:** add nodes to either mesh and delete nodes from either mesh; both reflected live in
   Topology + Cache.

(A later round — not this PRD — forces network isolation so direct fails and the relay actually carries
the cross-mesh write.)

## 9. Acceptance criteria
- ONE mesh + `+broker` → the broker appears in all five surfaces (Topology, Messages, Boot Waterfall,
  Timeline, Cache).
- TWO meshes locally (no Docker); `mesh1.gateway1` write-sim reaches `mesh1.broker1` and `mesh2.broker1`,
  both resolved from the cache and connected directly.
- Add a node to either mesh; delete a node from either mesh — both reflected live.
- No bridge nodes anywhere; node names are `mesh1.broker1`-style; no `<unspawned>` for live nodes; no
  duplicate node_ids in topology; CPU/RAM bars present.

## 10. Critical files
- `crates/rafka-node-base/src/lib.rs` — add `location` to the gossiped digest; reuse `run_gossip` /
  `live_digests` / `topic_membership`; have gateways subscribe to all meshes via `RAFKA_OBSERVER_MESHES`
  + a cross-mesh seed; the cache-directory accessor; **remove `Role::Bridge` + `RAFKA_BRIDGE_TARGET_MESHES`**.
- `crates/rafka-mesh-transport/src/lib.rs` — keep relay env-driven (the relay client wiring for the
  later forced-path round); default direct/relay-disabled.
- `admin-ui/src/main.rs` (Rust/axum) — server side: `GET /api/topology-cache` (NEW), deterministic
  naming on spawn, drop `bridge` spawn/render, kill removes from cache + topology, dedup topology by
  node_id. The HTTP/route/state layer only.
- `admin-ui/web/src/` (**React + react-flow + Vite** — this is the actual UI, NOT inline HTML) — add the
  Cache tab/view, add the `+relay` button, drop the `+bridge` button, render the direct cross-mesh edge.
  Rebuild with `npm run build` → `web/dist` (served by `ServeDir`, `main.rs:4351`). The admin-ui binary
  must be re-launched after a rebuild to pick up new assets.
- The write-sim lives on the `gateway` binary, driven by cache lookup + direct connect.

## 11. Verification — Playwright screenshots per phase (the durable proof)

The admin-ui is the proof, and the proof is **captured**, not just watched. A **Playwright** harness
(`docs/plans/mesh-v2/verify/`, headless Chromium) drives the live admin-ui and saves PNGs per milestone
into `docs/plans/mesh-v2/verify/screenshots/<phase>/`. A milestone is not closed until its screenshots
exist and show the required surfaces populated (not empty, no `<unspawned>`, no error banners).

- **Phase 0 (baseline harness):** screenshot the current working admin-ui to prove the toolchain drives
  the real UI end-to-end before any mesh-v2 code lands.
- **Phase 1 (one mesh + `+broker`):** one PNG each of Topology, Messages, Boot Waterfall, Timeline, and
  the Cache view, all showing `mesh1.broker1`.
- **Phase 2 (two meshes, cross-mesh cache):** Topology showing both meshes + the direct gateway→broker
  cross-mesh edge; Cache view holding both meshes' entries; Messages showing the cross-mesh write-sim.
- **Phase 3 (add/delete):** before/after PNGs of a node added to a mesh and a node deleted (gone from
  Topology + Cache, no ghost).

Jaeger backs Boot Waterfall + Timeline; gossip backs Topology + Messages + Cache. Workspace stays
warning-free (`cargo check --workspace --tests --no-default-features`).

## 12. UI stack — React (ruling, 2026-05-31)

The admin-ui is a **React + react-flow SPA** built by Vite (`admin-ui/web/`, served from `web/dist`).
The old "plain HTML+JS only" wording of Golden Principle #4 was **dropped** by the user on 2026-05-31;
CLAUDE.md #4 now reflects this. **Build all UI work on the React app** — Cache tab, `+relay` button, drop
`+bridge` — in `admin-ui/web/src/` (TSX), then `npm run build` → `web/dist` and relaunch the binary. No
plain-HTML rewrite.
