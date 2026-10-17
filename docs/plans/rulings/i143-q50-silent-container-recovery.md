**Q50: A — Luke, 2026-10-17.** Lift the cut before the declared hold by construction: the cell asserts its planned silence is under `RDM_HUNG_HOLD_FLOORS × floor` before installing it, collects the three route observations concurrently, and keeps every provider, identity, value, routing and bound assertion. No runtime change, no bound change. The expired-hold container case (B's proof, which this flame showed by accident) is an s88 follow-on cell, not this landing.

# Q50 — same-birth container healing across the R-FM1 deadline

Pending Luke. Candidate `425a27d9`, 2026-10-17. Issue: rafka-v2 #2784; delivery record: i143 e12.s88.

## 1. Spec

[node-recovery.md, Detection and proof, step 4](../../architecture/lifecycles/node-recovery.md): “When the hold expires with the provider still reporting `Running`, the mesh-primary submits the recovery Build for that birth whose first step is `TerminateRuntime { node_id, incarnation, handle }`”. The same step continues ordinary recovery in the same attempt with a new node.id at the same path.name. Owner: the member’s mesh-primary. Sequence: unanswered birth → TRUE OFFLINE → provider Running → held-running → expiry → exact terminate and exit proof → new birth.

The hold is `RDM_HUNG_HOLD_FLOORS` staleness floors, default 10. This cell declares a 3 s floor, so the hold is 30 s. The cell’s same-birth assertions cover a different case: a temporary silence healed while the held runtime survives. No page grants a Running birth immunity from the expired hold.

## 2. Issue

`container_node_unheard_heals_serves_same_birth` fails its 150 s healed-view wait after the target is recovered during its silence; 1/1 exercised runs on `425a27d9` (the earlier `700ab971` run failed the setup read before this phase), with the other four container fault cells green.

## 3. Flame

**Observations.** [Held Running immediately before submission](http://localhost:16686/trace/92c86f4de4c21a14e33aa13552296d89); [terminate and completed recovery](http://localhost:16686/trace/152a6c37517d0c0efdd97aa79411e186). Both traces were re-imported from canonical evidence and verified through Jaeger’s query API (1 and 55 spans). Verbatim span records:

```json
{"attributes":{"busy_ns":"28800","code.filepath":"crates/rdm-admin/src/admin/recovery.rs","code.lineno":"321","code.namespace":"rdm_admin::admin::recovery","idle_ns":"12048","incarnation_id":"kqxnhrqqsqny","node":"mesh2.rpc.1","node_id":"snxwmyma5r8c","outcome":"recorded","recorder":"mesh2.admin.1","since":"1792273191596","thread.id":"17","thread.name":"tokio-rt-worker","verdicts":"62"},"end_unix_nano":1792273222092338380,"events":[{"attributes":{"code.filepath":"crates/rdm-admin/src/admin/recovery.rs","code.lineno":"332","code.namespace":"rdm_admin::admin::recovery","level":"INFO","target":"rdm_admin::admin::recovery"},"name":"recorded held-running verdict","time_unix_nano":1792273222092334860}],"name":"rdm.node_admin.node.update.via-held-running","parent_span_id":"","service":"rafka-node-admin","span_id":"b6b4e9b9a878ff66","start_unix_nano":1792273222092299388,"trace_id":"92c86f4de4c21a14e33aa13552296d89"}
{"attributes":{"attempt":"1","build_id":"bid_rdrr-h9er-5sx5","busy_ns":"3415519","code":"OK","code.filepath":"crates/rdm-admin/src/deployment/pipeline.rs","code.lineno":"1216","code.namespace":"rdm_admin::deployment::pipeline","detail":"complete","elapsed_ms":"2219","idle_ns":"2216471467","io_calls":"2","io_ms":"4","io_queued_ms":"0","node":"mesh2.rpc.1","outcome":"complete","provider":"Container","step":"TerminateRuntime","thread.id":"11","thread.name":"tokio-rt-worker"},"end_unix_nano":1792273224353815243,"events":[{"attributes":{"code.filepath":"crates/rdm-admin/src/deployment/pipeline.rs","code.lineno":"2460","code.namespace":"rdm_admin::deployment::pipeline","level":"INFO","node":"mesh2.rpc.1","released_after_ms":"1","target":"rdm_admin::deployment::pipeline"},"name":"the runtime exited and the operating system released its addresses","time_unix_nano":1792273224353771867}],"name":"rdm.node_admin.deployment.update.via-step","parent_span_id":"47094308ea836f5f","service":"rafka-node-admin","span_id":"4152660651495a22","start_unix_nano":1792273222133930657,"trace_id":"152a6c37517d0c0efdd97aa79411e186"}
{"attributes":{"attempt":"1","build_id":"rdrrh9er5sx5","busy_ns":"7231237","code.filepath":"crates/rdm-admin/src/admin/recovery.rs","code.lineno":"717","code.namespace":"rdm_admin::admin::recovery","disposition":"created","exit_code":"","exit_proof":"held-running-expired","idle_ns":"2692170661","incarnation_id":"kqxnhrqqsqny","mesh":"mesh2","node":"mesh2.rpc.1","node_id":"snxwmyma5r8c","outcome":"complete: attempt 1","submitter":"mesh2.admin.1","thread.id":"17","thread.name":"tokio-rt-worker"},"end_unix_nano":1792273224791774406,"events":[{"attributes":{"code.filepath":"crates/rdm-admin/src/admin/recovery.rs","code.lineno":"709","code.namespace":"rdm_admin::admin::recovery","level":"INFO","target":"rdm_admin::admin::recovery"},"name":"the mesh primary opens and runs the recovery of its proven-exited member itself","time_unix_nano":1792273224791767190}],"name":"rdm.node_admin.build.update.via-node-recovery","parent_span_id":"","service":"rafka-node-admin","span_id":"9d2188a5b9da8887","start_unix_nano":1792273222092372940,"trace_id":"152a6c37517d0c0efdd97aa79411e186"}
```

Evidence: `/home/admin/rust-distributed-mesh-deploy/kept/s88-q50/` (estate, cell log, selected raw records, OTLP import and queried traces). The held row’s `since=1792273191596` ms and submission at `1792273222092` ms are 30.496 s apart. `TerminateRuntime` completes in 2219 ms; the same trace records completed recovery, `exit_proof=held-running-expired`.

## 4. Code

**Observations (commit `425a27d9`).**

- [code] `crates/rdm-admin/src/admin/records.rs:12`, `:24`: default ten floors; hold milliseconds multiply the declared staleness floor.
- [code] `crates/rdm-admin/src/admin/recovery.rs:333`, `:364`: expiry submits the ordinary held-running recovery; no container exception.
- [code] `crates/rdm-test/tests/chaos/container_fault_backend.rs:325`, `:360`: three rounds call the route probes sequentially while the cut remains installed; lift follows all rounds and their view/provider reads.
- [code] `crates/rdm-test/tests/chaos/container_fault_backend.rs:372`, `:393`, `:402`: convergence requires the original births, then the original containers and value served by the original birth.
- [flame] `152a6c37517d0c0efdd97aa79411e186`: mesh2.admin.1 recovers mesh2.rpc.1 (`snxwmyma5r8c@kqxnhrqqsqny`) on the expired hold, in Build `rdrrh9er5sx5`, attempt 1.

### Inferred

- [inferred] The recovery precedes lifting the cut: submission starts at 1792273222092 ms; the final no-route probe starts at 1792273222653 ms, and the loop lifts the cut only after it returns and the remaining observations finish. This inference combines the probe timestamps with the cited loop order.
- [inferred] The original-birth convergence cannot complete after this correctly executed recovery; the 150 s failure is a conflicting cell premise, not evidence that recovery failed.

## 5. Delta

No spec covers preserving a Running birth beyond the held-running deadline while its probes remain unanswered; the container cell requires that preservation after holding its cut across the deadline.

## 6. Arms

Every arm assumes R-FM1’s hold, terminate and recovery rule stands.

- **A:** Keep the same-birth healing proof, collect independent route observations concurrently, and require lifting the cut before the declared hold deadline; changes cell sequencing, retaining its provider, identity, value, routing and bound assertions. The expiry behavior remains the existing `recovery.rs:333–364` path and its recovery cells.
- **B:** Keep the long silence and prove its expired-hold termination, provider exit proof and new birth with retained value; changes this cell’s name and assertions to the R-FM1 case, with a separate pre-expiry same-birth healing control. Runtime path remains `recovery.rs:333–364`.

A keeps this cell’s original healing purpose. Neither arm changes a runtime hold or a test timeout.
