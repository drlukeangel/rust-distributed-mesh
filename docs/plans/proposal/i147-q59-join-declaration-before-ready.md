# Q59 (asked as Q58, renumbered by the lead) — recording an app declaration before the Day-0 writer is Ready

## 1. Spec

`gossip.md` §8: “Its mesh's node-admin records the declaration beside the row, joins every type it does not already hold before it answers the join”. Owner: the receiving mesh node-admin; declaration and physical joins precede the Join answer. `node-identity-and-state.md` §2.5: “each mesh-primary writes its own mesh's rows into it.” `node_rows.rs` documents R-OW1's existing Day-0 exception for the bootstrap admin's own name claim before Ready. No page specifies how that pending bootstrap authority records a launched birth's app declaration while its own hydration hook is held.

The existing `hydrate_before_ready_admin` cell requires the deployed admin to Join the Pending Day-0 admin, then block its own hydration until that authority is Ready. Its bounds and sequencing remain unchanged.

## 2. Issue

The launched admin's Join is refused by the new declaration write while the Day-0 hook is held; 1/1 executions in the first e5 gate (`820166de`), all five Join attempts refused.

## 3. Flame

Observations: [launched admin and five refusals](http://localhost:16686/trace/212ac8f8945c0cb9769b9c21f98a8aef), [Day-0 hydration remains held](http://localhost:16686/trace/2d41dfc894866fcb87ca694557bbf880). Imported original evidence and retrieved both traces: 46 and 12 spans; raw and query results retained at `/home/admin/rust-distributed-mesh-deploy/kept/i147-e5-join-before-ready/`.

The following span names and attributes are copied verbatim from the evidence (timestamps and event arrays omitted):

```json
{"name":"rdm.mesh.entry.update.via-pull-attempt","trace_id":"212ac8f8945c0cb9769b9c21f98a8aef","attributes":{"anchor":"0bf1dedb6a","attempt":"1","busy_ns":"2672","code.filepath":"crates/rdm-node/src/join.rs","code.lineno":"68","code.namespace":"rdm_node::join","elapsed_ms":"27","idle_ns":"27017957","node":"mesh1.admin.2","outcome":"unreached: not-ready: NotReady { reason: \"mesh1.admin.1: the app declaration could not be joined and recorded: mesh1.admin.1 (k2gp61dh1j2q) is not the writer of mesh1's node rows and refuses the row at mesh1.admin.2: mesh1 has no primary node-admin and the view holds no fabric-primary to write them for it\" }","step":"not-ready","thread.id":"2","thread.name":"hydrate_before_ready_admin::an_admin_launched_by_an_admin_whose_hook_has_not_passed_is_pending_until_it_does"}}
{"name":"rdm.mesh.entry.reject.via-membership-pull-failed","trace_id":"212ac8f8945c0cb9769b9c21f98a8aef","attributes":{"attempts":"5","busy_ns":"10640","code.filepath":"crates/rdm-node/src/join.rs","code.lineno":"112","code.namespace":"rdm_node::join","idle_ns":"12832","node":"mesh1.admin.2","reason":"not-ready: NotReady { reason: \"mesh1.admin.1: the app declaration could not be joined and recorded: mesh1.admin.1 (k2gp61dh1j2q) is not the writer of mesh1's node rows and refuses the row at mesh1.admin.2: mesh1 has no primary node-admin and the view holds no fabric-primary to write them for it\" }","thread.id":"2","thread.name":"hydrate_before_ready_admin::an_admin_launched_by_an_admin_whose_hook_has_not_passed_is_pending_until_it_does"}}
{"name":"rdm.mesh.node.update.via-hydrate-attempt","trace_id":"2d41dfc894866fcb87ca694557bbf880","attributes":{"attempt":"1","authority":"none: the Day-0 root: no authority exists before this node","busy_ns":"5232811","code.filepath":"crates/rdm-node/src/app_hydration.rs","code.lineno":"493","code.namespace":"rdm_node::app_hydration","idle_ns":"4247581543","incarnation_id":"gzgnkv8nwhb2","inherited":"false","node":"mesh1.admin.1","node_id":"k2gp61dh1j2q","thread.id":"2","thread.name":"hydrate_before_ready_admin::an_admin_launched_by_an_admin_whose_hook_has_not_passed_is_pending_until_it_does"}}
```

## 4. Code

Observations on candidate `820166de` (the same declaration path remains at RED commit `c806f20d`):

- [code] `crates/rdm-admin/src/node_rows.rs:501`: `declare_app_types` first requires `writer_check`; this is the new e5 gate on Join.
- [code] `crates/rdm-admin/src/node_rows.rs:210`: `writes_for` requires the mesh seat or acting fabric seat in the current topology; the pending Day-0 topology holds neither primary. The check does not test Ready status directly (`topology.rs:202-233`); it fails because this view does not yet name a holder.
- [code] `crates/rdm-admin/src/admin/boot.rs:1387`: Join awaits that write before learning/joining the declaration.
- [code] `crates/rdm-admin/src/join.rs:326`: an error from that callback produces NotReady before install and deployment check-in.
- [code] `crates/rdm-test/tests/hydrate_before_ready_admin.rs:145`: the rig explicitly registers the exact deployment, then awaits its Join before releasing the root's hook; it does not seed the deployed birth's claimed node row.
- [flame] The receiving root is `mesh1.admin.1`, node `k2gp61dh1j2q`; all five attempts name the same refusal, no eligible mesh or fabric primary.

### Inferred

- [inferred] Waiting for Ready inside Join would deadlock this declared sequence: the cell releases the root hook only after the launched admin has joined and blocked on hydration. Raising a timeout cannot resolve the authority gap.

## 5. Delta

No spec covers this step; code requires a seat writer named in its current topology to record the app declaration before a launched admin can Join its still-Pending Day-0 authority.

## 6. Arms

Every arm assumes the exact deployment has already passed Join's birth verification, and preserves one node row, immutable per-birth types, existing hydration ordering and bounds.

- A: explicitly permit the verified deployment authority to append only the app declaration to that birth's already-claimed row before Ready, including Day 0; seed the existing cell's legitimate claim as the deployment pipeline does, and refuse a missing/mismatched claim. Wire unchanged; publisher unchanged; timer unchanged; tier is the verified deployment authority for this field only; no second record.
- B: retain the current-view seat writer requirement and change the lifecycle contract to forbid a second admin's Join until Day-0 hydration completes. Wire unchanged; publisher unchanged; timer unchanged; tier unchanged; changes the existing hydration sequence and its cell.
