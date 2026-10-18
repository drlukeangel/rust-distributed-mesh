**Q53: A — Luke, 2026-10-17.** (Codex's packet was numbered Q51; the i144 dev holds Q51 and Q52, so this is Q53.) Codex traces the acknowledged status put through the shared root `status/<mesh>/` to the successor's fold (`crates/rdm-admin/src/status_rpc.rs` SeatFold), names the cause (a put that never landed, a cleared row, a wrong key, or a read defect), and fixes the root in that existing path. RED first: the exact-birth successor cell (ReadyForTraffic applied and acknowledged, seat taken, the successor's first complete version carries ReadyForTraffic for that birth) and the fold flame with a non-zero row count. No coping layer (no re-fold, no wait). Codex lands first, the i144 owner rebases. Luke's permission for this mesh change is this ruling. Then s88 through the land script, then i147 e5.

# Q53 (filed as Q51) — successor publishes Pending over live births whose ReadyForTraffic was applied

Asked 2026-10-17. Unruled. Evidence: s88 concurrent release `5b485f228750f0b077a89132b1fc4541eb82e841`, rebased on main `ac1acf0d`. No product change for this defect is included.

## 1. Spec

[node-status-heartbeat-connections.md §A.3](node-status-heartbeat-connections.md): “the successor acquires the held member statuses and publishes a complete projection”. [states.md, Who sets each state](../../architecture/lifecycles/states.md#who-sets-each-state) makes the mesh-primary set Pending from node.joined and ReadyForTraffic from node.started. [fabric-authority.md §7](../../architecture/fabric-authority.md#7-mesh-primary-takeover-is-hydration-not-transition): “A successor mesh primary reads/hydrates the Mesh lifecycle state that already exists and adopts that exact state locally.” Owner: the successor mesh-primary. Sequence: acquire the predecessor’s held statuses → publish the complete projection; authority changing is no new lifecycle transition.

## 2. Issue

`container_peer_mesh_lost_recovers_under_same_mesh_id` fails its **healthy setup before its fault** at `typed.rs:425`: the same two RPC births remain Pending at the 60 s settling bound, after a successor’s full replaced their earlier ReadyForTraffic; 1/1 in release `5b485f22` (112.01 s total), versus green in the preceding release `127fb05e`. Tracking: i143 s88, existing acceptance job `i143-2784-chaos-container` / issue #2784. This is separate from Q50’s pre-expiry cut timing.

## 3. Flame

Observations, imported and queried from retained raw JSONL:

- [Ready accepted by the authority](http://localhost:16686/trace/ca8b93b5ac5fdd6994aaccc1114186cf): `rdm.node_admin.status.update.via-declaration`, `node="mesh2.admin.1" op="declare-node-state" outcome="applied" receiver_is_primary="true" sender="mesh2.rpc.1" state="ReadyForTraffic"`.
- [Earlier complete projection](http://localhost:16686/trace/e83755251052bae5522808e8bbace483): `rdm.mesh.membership.update.via-member-state`, `incarnation_id="k6334480xkg5" member="mesh2.rpc.1" member_id="k5n5e6agt1vx" node="mesh1.admin.1" prior="Pending" status="ReadyForTraffic"`.
- [Successor fold](http://localhost:16686/trace/15cb9cd91901c07ab8111839908d878b): `rdm.node_admin.status.resolve.via-seat-taken`, `mesh="mesh2" node="mesh2.admin.1" node_declared_rows="0" node_id="fysqe43rsgr2"`.
- [Successor full and regression](http://localhost:16686/trace/1b56aa5ce40f7d173648b8955b64b6ef): `rdm.mesh.membership.update.via-snapshot-installed`, `new_revision="true" node="mesh1.admin.1" publisher="mesh2.admin.1:fysqe43rsgr2@cg8kefshmsx4" snapshot_id="1" topology_version="1"`; then `rdm.mesh.membership.update.via-member-state`, `incarnation_id="k6334480xkg5" member="mesh2.rpc.1" member_id="k5n5e6agt1vx" node="mesh1.admin.1" prior="ReadyForTraffic" status="Pending"`.

The same full regresses `mesh2.rpc.2` (`t6a8bdfcknng`, incarnation `7exnccf8xwd0`). Raw span lines, whole traces, gate log, and queried Jaeger responses: `/home/admin/rust-distributed-mesh-deploy/kept/s88-q51/`; import/verification log: `/home/admin/rust-distributed-mesh-deploy/s88-q51-flames.log`. The earlier RPC Ready install is at `1792278426383750612 ns`; the regression at `1792278462236076439 ns`, 35.852 s later. The publisher’s node.id changed; both RPC births did not.

## 4. Observations (commit `5b485f22`)

- [code] `crates/rdm-test/tests/chaos/container_fault_backend.rs:596`: the 60 s ready-view wait precedes the cell’s exact-container kills (`:618`); this failure is not a consequence of its injected fault.
- [flame] The authority answered applied; the entry admin installed Ready, then the replacement publisher’s first complete version installed Pending for the same exact birth. Its seat-fold span reports zero node declared-state rows.
- [code] `crates/rdm-admin/src/status_rpc.rs:224`: SeatFold reads `storage.node_declared()`, folds it and adopts it into the digest book (`:234-238`), before the topology install (`admin/records.rs:944`). This is the existing acquisition path; no replacement path is proposed.
- [code] `crates/rdm-admin/src/status_storage.rs:9`: the row contract is acknowledged put before Applied and shared-root `status/<mesh>/` read by the successor.
- [code] `crates/rdm-transport/src/membership.rs:5138`: an own-mesh birth with no held exact-birth status is handed out as Pending. `:2636` spans the peer’s installed status; it must not invent Ready from the birth’s heartbeat.

### Inferred

- [inferred] The gap is before the successor’s complete publication, not a stale frame being misclassified: its publisher is a new birth and the first version is admissible. Why the successor’s declared-row read is empty despite earlier Applied is **not yet established**. A missing/cleared/wrongly addressed durable row and a read defect remain distinct possibilities; the zero-row span alone does not select one.

## 5. Delta

Spec says the successor acquires the held member statuses then publishes its complete projection; flame shows its complete projection without those held statuses, replacing the same live births’ ReadyForTraffic with Pending.

## 6. Arms

Every arm assumes the settled status-authoring and takeover rules stand; no heartbeat establishes lifecycle state and no receiver fabricates a replacement status.

- **A:** Codex traces the acknowledged status put through the shared root and the successor’s fold, fixes the root in that existing path, and adds the exact-birth successor cell and flame ahead of s88’s landing. Wire: unchanged; publisher: the existing successor; timer/bounds: unchanged; tier: mesh-primary; fork: none. Q50’s no-runtime-change restriction remains for the cut itself; this is an explicit additional product fix.
- **B:** The i144 owner fixes that existing shared-status acquisition path separately; Codex keeps s88 held, then rebases and runs the concurrent gate and land script. Wire/publisher/timer/tier/fork: as A; ownership and landing order change.

No page replacement is proposed. The full gate RED and Q50 RED logs stay kept. i147 e5 follows s88’s successful landing.

Approval boundary: Q50 explicitly says “No runtime change.” The canon in `/home/admin/rafka-v2/CLAUDE.md` says “No agent (lead or seat) changes gossip or the mesh without Luke’s explicit permission.” A fixes the existing product path; it needs that additional authorization. The [ruling-packet skill](/home/admin/rafka-v2/.claude/skills/ruling-packet/SKILL.md) supplies the packet format, not a separate approval requirement.

---

**Q54: A — ratified by Luke, 2026-10-17, after it landed in `8bd6681d` on the lead's recommendation.** `status/<mesh>/declared` opens on the shared root and its write completes before Applied, including before state-commit; same row, key and format; the fold ignores a row for a birth it does not hold; the states.md Rounds line landed with the code. The scratchpad-inheritance question for other held rows is the i144 owner's, under a new number.
