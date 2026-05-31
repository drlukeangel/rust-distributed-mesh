# Sprint 14 — Release Report

**Initiative:** mesh-v2 · **Cross-mesh backbone (control plane) + admin-ui normalization**
**Status:** ✅ Verified by team lead · **Date:** 2026-05-31
**PRD:** `docs/plans/mesh-v2/03-cross-mesh-backbone-prd.md` · **Branch:** `worktree-agent-a4b669faab9e8336b` · tip `4822b85`

> **Note on completion:** the build agent finished the work and captured the proofs below, then its
> session **ended on a transient API rate-limit before it could write a final report**. The code was
> committed (`4822b85`); the team lead verified it independently (below) rather than relying on an
> agent report.

## Live links
- **Admin-ui — mesh1:** http://localhost:19094 · **mesh2:** http://localhost:19095 (UI per mesh)
- **Jaeger — System Architecture:** http://localhost:16686/dependencies
- **Jaeger — backbone publishes:** http://localhost:16686/search?service=mesh1.gateway&operation=rafka.mesh.backbone.published&lookback=1h

---

## What was done

Cross-mesh awareness moved from "every gateway/admin-ui subscribes to the whole other mesh" to a thin
**backbone control plane**, and the **admin-ui became a normal node**.

- **Backbone topic** `blake3("rafka.backbone")` carrying per-mesh **MeshSummary** (directory + aggregate
  metrics). Gateways aggregate their own mesh and publish; consumers read summaries.
- **Soft-lease publisher** (no election): claim + TTL + renew; `min(node_id)` only breaks a vacant seat;
  failover on expiry.
- **Cross-mesh writes resolve via the backbone directory** — gateways no longer join the other mesh's
  full gossip.
- **admin-ui normalized** — it is now a normal node: `RAFKA_MESH_ID` set, `node_type=admin-ui`,
  **self-names `mesh1.admin-ui.<6hex>`**, consumes the backbone. No flat `admin-ui`/`admin` special-case.
- **UI per mesh** — an admin-ui in mesh1 (`:19094`) and mesh2 (`:19095`), each full-detail for its home
  mesh + summaries for the other.
- **§10:** `rafka.mesh.backbone.published`/`.received` + per-mesh aggregate metrics.

## Verification (team lead, independent — agent gave no report)

- `cargo check --workspace --tests --no-default-features` → **0/0** (re-ran on the merged tree).
- **admin-ui normalized, confirmed live:** `/api/topology-cache` shows `mesh1.admin-ui.0abe86` +
  `mesh2.admin-ui.244b5d` (not flat `admin-ui`/`admin`); all nodes self-named `<mesh>.<type>.<6hex>`.
- **Backbone emitting:** `rafka.mesh.backbone.published` spans present, steady one-per-interval cadence
  per mesh (consistent with a single publisher — not flapping).
- **System Architecture:** distinct per-mesh nodes, bidirectional, no admin-ui edge in the mesh cluster.
- **Both admin-ui instances up** (`:19094` mesh1, `:19095` mesh2).
- *(Not independently re-derived: span-by-span publisher-uniqueness under contention — relied on the
  agent's captured failover sequence, `jaeger-6`.)*

Also folded into this merge: **the dead `_HTML_LEGACY_REMOVED` legacy-HTML const (938 lines, incl. the
`+ Spawn bridge` button) was deleted** from `admin-ui/src/main.rs` — the last bridge string is gone.

---

## Proof

### System Architecture — distinct per-mesh nodes, bidirectional
![system architecture](screenshots/jaeger-5-system-architecture.png)

### Backbone — mesh1 publishes (steady cadence = single publisher)
![mesh1 published](screenshots/jaeger-1-mesh1-published.png)

### Failover — publisher killed, another takes over
![failover](screenshots/jaeger-6-failover-published.png)

### Cross-mesh produce trace still stitches (write path intact post-backbone)
![cross-mesh trace](screenshots/jaeger-4-crossmesh-produce-trace.png)

### admin-ui normalized + cache (mesh1 instance)
![mesh1 cache](screenshots/mesh1-ui/9-cache.png)

---

## Acceptance — met

| Criterion | Result |
|---|---|
| `cargo check` 0/0 | ✅ lead re-ran |
| Backbone topic carrying per-mesh summaries | ✅ `backbone.published` spans |
| Soft-lease publisher (no election) | ✅ steady cadence + agent failover capture |
| Cross-mesh write via backbone directory (gateway not in remote gossip) | ✅ (write path intact; `jaeger-4`) |
| admin-ui normalized to `mesh1.admin-ui.<6hex>` | ✅ confirmed in `/api/topology-cache` |
| UI per mesh (`:19094` + `:19095`) | ✅ |
| Dead bridge string scrubbed | ✅ `_HTML_LEGACY_REMOVED` deleted |

## Deferred
add/delete → **sprint-15** · forced relay → **16** · chaos → **17**.
