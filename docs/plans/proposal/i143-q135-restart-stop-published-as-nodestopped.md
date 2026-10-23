## Q135: a restart's stop publishes NodeStopped, so a lost executor leaves the restarted birth refused forever

### 1. Spec
gossip.md :271-272: "Inside node.restart, the stop's stopped is published as NodeRestarting, not NodeStopped;
peers hold the instance and expect it back. NodeRestarting stays until node.started of the same `(node.id,
incarnation)` arrives". gossip.md :274-276: "NodeStopped stays in the in-flight overlay until the next lifecycle op
on the same `(node.id, incarnation)`. node.start clears it at node.joined". node-restart.md :21: a resumed restart
"continue[s] at start". Owner: the mesh-primary (publisher). Sequence: NodeRestarting -> stop -> start ->
node.started ends the hold.

### 2. Issue
A restart resumed by a successor executor waits 30 s for a Ready that never comes and fails WaitForNodeReady "no
digest from mesh2.rpc.2 for this birth"; seen at gates e7s3 (2026-10-16, 10-17 x4), e4s3-f8b0f6f5 (10-21),
e6s8-eee4cb215 and e4s8-1b4c68f91 (10-23); `build_op::a_lost_executor_ends_the_inner_call_indeterminate_and_the_successor_is_claimed_only_after_proof`.
First failure with its artifacts kept.

### 3. Flame
Spans: /home/admin/rust-distributed-mesh-deploy/kept/gate-gates-e4s8-1b4c68f91/artifacts/fabric-build/a_lost_executor_ends_the_inner_call_indeterminate_and_the_successor_is_claimed_only_after_proof/spans/
CONTRADICTION: inside node.restart (attempt 3, NodeRestarting published at 4.264 s), the stop's stopped is published
as NodeStopped, `operation=node.stop:mesh2.rpc.2`, to every member:
```
4.2893 rdm.mesh.membership.remove.via-node-stopped holder=mesh2.admin.1 node=mesh2.rpc.2 operation=node.stop:mesh2.rpc.2 was_held=true
4.2899 rdm.mesh.membership.remove.via-node-stopped holder=mesh2.admin.2 publisher=mesh2.admin.1:rgj3m0z29k10@4smr2zt3xt93
4.2910 rdm.mesh.membership.remove.via-node-stopped holder=mesh2.rpc.1  publisher=mesh2.admin.1:rgj3m0z29k10@4smr2zt3xt93
```
mesh2.rpc.2 rejoins and starts through mesh2.admin.1 (node-joined 4.378 at mesh2.admin.1 only, ready 4.386); mesh2.admin.1
is lost. The successor mesh2.admin.2 takes the seat (7.340), resumes at start, takes node.started (7.947
`command-sent Started`) and then refuses every digest of the birth to the end (about two per second, 4.38 s to 38 s):
```
rdm.mesh.membership.reject.via-stopped-birth node=mesh2.admin.2 member=mesh2.rpc.2 door=heartbeat
7.976 rdm.node_admin.deployment.update.via-step step=WaitForNodeReady outcome=failed reason="no digest from mesh2.rpc.2 for this birth"
```
mesh2.rpc.1 refuses the same digests (`via-stopped-birth`, holder mesh2.rpc.1).

### 4. Observations (commit 1b4c68f91)
- [code] `crates/rdm-admin/src/deployment/pipeline.rs:2017-2020`: the restart stops the birth with `StopEnd::Park { publish_stopped: false }`, "`NodeRestarting` holds it, so no `NodeStopped` is published for it".
- [code] `crates/rdm-admin/src/admin/runner.rs:132-147` (`stopped_publisher`): the status authority publishes NodeStopped once it applies a birth's Leaving that names the operation which stopped it, whatever pipeline sent the stop.
- [flame] the restart's stop command and the birth's declaration name `node.stop:mesh2.rpc.2` (`via-stop-admitted`, `via-node-stopped`).
- [code] `crates/rdm-transport/src/membership.rs` `stopped_refuses`: every digest of a stopped birth is refused until `take_joined` (node.joined), a later incarnation, or a deletion ends the stop; `started()` (node.started) closes an open restart and does not end a stop.
- [flame] the only node.joined of the restarted birth was taken at mesh2.admin.1 (4.378), the lost executor; no member heard it.
#### Inferred
- [inferred] in a run whose executor lives, its node.joined reaches the members and ends the stop, which is why the cell passes alone; the failure needs the executor lost between the stop and that publication — from the six sightings' timing and the passing reruns.

### 5. Delta
Spec says a restart's stop is published as NodeRestarting, not NodeStopped; flame shows NodeStopped published for it by the status authority.

### 6. Arms
Every arm assumes NodeRestarting stays the restart's hold and ends at node.started of the same birth.
- A: the status authority publishes no NodeStopped for a birth whose restart is open in its book (the stop of a restart is held by NodeRestarting) — changes publisher (runner.rs `stopped_publisher` reads the open restart overlay before it publishes).
- B: the restart's stop command and the birth's declaration name the restart's operation (`node.restart:<path>`); the authority publishes NodeStopped only for a `node.stop:` operation — changes wire content (the command's operation string) and publisher.
- x: the delta itself is the fix — changes publisher.
