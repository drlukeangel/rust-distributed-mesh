# Proposal: the deployment provider reports the runtime's actual state

## Problem

The deployment provider launched every runtime and holds its exact handle: a process's pid with its
start identity, a container's id. [node.runtime.inspect](../../architecture/lifecycles/node-runtime-inspect.md)
is "the only thing in the design that proves a runtime is gone". But the provider's answer set cannot
say that the operating system holds a runtime stopped, so the provider reports a stopped runtime as
running, and the mesh infers from silence what the provider could read directly.

| where | what the code does |
|---|---|
| `crates/rdm-admin/src/deployment/provider.rs:298` | `DeploymentStatus` is `Running`, `Exited { code }` or `Unknown`. There is no stopped runtime |
| `crates/rdm-admin/src/deployment/process.rs:443` `inspect` | answers `Running` for any pid whose tasks are alive |
| `crates/rdm-admin/src/deployment/process.rs:127` `alive` | reads `/proc/<pid>/task/*/stat` only to rule out a zombie (`is_zombie`, :142). It never reads the state letter, so a process stopped by SIGSTOP (state `T`) is `Running` |
| `crates/rdm-admin/src/deployment/container.rs:621` `inspect` | maps docker's `paused` (`docker pause`, which freezes the container's cgroup) to `Running` |
| product code | reads the `T` state nowhere. Only the test kit does (`crates/rdm-test/src/process_faults.rs:330` `proc_state`) |

What the mesh does instead ([node recovery](../../architecture/lifecycles/node-recovery.md) steps 1-4):
the node's digests stop past the staleness floor; the mesh-primary marks it `PendingReconnect`; half a
floor later it runs [node.tickle](../../architecture/lifecycles/node-tickle.md); no answer, and none
again a floor later, is TRUE OFFLINE; only then does it ask the provider, which answers `running`; it
records `held-running` and opens the R-FM1 hold of `RDM_HUNG_HOLD_FLOORS` floors (10, 300 s at the
30 s floor); at the hold's end it submits `TerminateRuntime` and a recovery that creates a new node.id
at the path.name. A runtime the operating system holds stopped is treated as hung, and terminated and
replaced if it stays stopped past the hold.

## Evidence: R-shape cell C17

`rshape_burn_in::mock_drain_and_retire_preserve_status_and_terminal_proof`, release suite, trace
`3100a0ae262f62d82e02953ef3c98d36` (the restart attempt). What the code did, in order:

```text
1      The test (hold_restart) sends SIGSTOP to mesh2.compute.2's process (Held::hold → process_faults SigFault::Stop).
2      The test sends Write::Restart(mesh2.compute.2) to the fabric-primary, mesh1.admin.1.
3      mesh1.admin.1 opens attempt 2 of the accepted Build.
4      mesh1.admin.1 dispatches build.attempt.run to mesh2's mesh-primary, mesh2.admin.1.
5      mesh2.admin.1 runs Pipeline::restart (pipeline.rs:1846):
5.1      mesh2.admin.1, in its NodeRestarting step, gossips NodeRestarting (lifecycle.restarting, runner.rs:116).
5.2      mesh2.admin.1 starts its StopNode step (stop_steps_as). Before it sends anything to mesh2.compute.2:
5.2.1      mesh2.admin.1 sets node.status = Leaving and calls sink.publish(node), writing Leaving into its own topology (pipeline.rs:2249).
5.2.2      mesh2.admin.1 calls lifecycle.leaving(op) → book.admitted(op, Leaving), setting its member status for mesh2.compute.2 to Leaving (runner.rs:151).
5.2.3      mesh2.admin.1 gossips a NodeLeaving frame on mesh2's channel and the backbone (runner.rs:148).
5.2.4      Only then does mesh2.admin.1 call send_command(Stop): one StopNode request on 0x1B to mesh2.compute.2 (node_commands.rs:180).
5.3    mesh2.admin.1's Node RPC client ends that StopNode call NotSent (connection timed out) at +3.2 s: the test's SIGSTOP (1) holds mesh2.compute.2's process, so it accepts no connection.
5.4    mesh2.admin.1's AwaitNodeLeft step fails: "stop-node was not admitted … no left reply can follow".
5.5    mesh2.admin.1 ends attempt 2 Failed.
5.6    mesh2.admin.1 runs neither StartNode nor WaitForNodeReady.
6      mesh2.admin.1 never withdraws the Leaving it set in 5.2.1–5.2.3.
7      mesh2.admin.1, and every node that received the NodeLeaving frame on mesh2's channel or the backbone, keep mesh2.compute.2 as Leaving.
8      mesh2.admin.1 runs no node.tickle and no node.runtime.inspect for mesh2.compute.2 during the test's 70 s SIGSTOP (no such span).
9      The test sends SIGCONT to mesh2.compute.2's process (Held::release → SigFault::Continue).
10     The test sends Write::Restart(mesh2.compute.2) again to mesh1.admin.1.
11     mesh1.admin.1 refuses it at its door with node-not-live (control_plane.rs:922 not_live), because mesh1.admin.1's view holds mesh2.compute.2 as Leaving.
```

Had mesh2.admin.1 asked the provider at 8, the provider would have answered `running`
(process.rs:443): it cannot say that the test's SIGSTOP holds the process.

## Proposal

1. `DeploymentStatus` gains `OsStopped`: the exact runtime exists and the operating system holds it
   stopped. The name keeps it apart from the lifecycle status `stopped`
   ([States](../../architecture/lifecycles/states.md#stopped-nodes)), which is a live process that
   answers `node.start`; an `OsStopped` runtime answers nothing but SIGCONT (for a container,
   `docker unpause`).
2. The process provider's `inspect` reads the state letter of every task in `/proc/<pid>/task/*/stat`,
   the file `alive` already opens: every live task in `T` (or `t`) is `OsStopped`; any live task in
   another state is `Running`; no live task is `Exited`, as today. The start-identity check is unchanged.
3. The container provider's `inspect` maps docker's `paused` to `OsStopped`.
4. `node.runtime.inspect` answers `running`, `os-stopped` or `exit-proven { code }`. `unavailable
   { reason }` stays not a result. The span's `verdict` gains `os-stopped`.
5. The caller that cannot reach a node asks the provider instead of inferring from silence:
   - a command to the node ends NotSent (the node never admitted it): the executor runs `node.tickle`
     on the exact `(node.id, incarnation)`; on `unreached` it runs `node.runtime.inspect`;
   - node recovery after a tickle ends `unreached`, as today.
6. On `os-stopped` the caller records the durable status-row fact `os-stopped { since }`, surfaced on
   `node.get` and never as a gossip `MemberStatus`, as `held-running` is. It issues no command:
   commands to a node stay Node RPC, and the provider sends no signal. It opens no R-FM1 hold and no
   `TerminateRuntime`: the provider has proven the runtime is stopped by the operating system, not hung.
7. When the runtime runs again, the node's returning digest (the event that already ends the R-FM1
   hold, node recovery step 4) reaches the caller. The caller tickles the node, takes `reached
   { Current }`, clears `os-stopped` and re-issues the command it holds over Node RPC: for a restart,
   `node.start` ([node.start](../../architecture/lifecycles/node-start.md)).
8. What stays inference: a runtime the operating system shows running that does not answer (a
   deadlock). No provider can see it; RPC silence and the R-FM1 hold remain the only signal for it.

## C17 under this proposal

```text
1      The test sends SIGSTOP to mesh2.compute.2's process.
2      The test calls node.restart(mesh2.compute.2) at the fabric-primary, mesh1.admin.1.
3      mesh1.admin.1 forwards node.restart down to mesh2's mesh-primary, mesh2.admin.1. It opens no Build attempt: no node comes or leaves.
4      mesh2.admin.1 runs the node.restart workflow on mesh2.compute.2:
4.1      mesh2.admin.1 gossips NodeRestarting (node-restart.md step 1).
4.2      mesh2.admin.1 sends mesh2.compute.2 the commands that mesh2.compute.2 executes: node.connections.delete {All}, node.mesh.leave, node.stop. mesh2.admin.1 sets no status itself.
4.3      mesh2.admin.1's command ends NotSent: the test's SIGSTOP (1) holds mesh2.compute.2's process.
4.4      mesh2.admin.1 ends the attempt Failed { step, reason }. mesh2.compute.2 reported nothing, so mesh2.admin.1 gossips no status for it.
4.5      mesh2.admin.1 runs node.tickle on mesh2.compute.2 (direct, then through up to two live peers of mesh2): unreached.
4.6      mesh2.admin.1 runs node.runtime.inspect on mesh2.compute.2's launch handle: the process provider reads state T and answers os-stopped.
4.7      mesh2.admin.1 records os-stopped { since } for mesh2.compute.2 and issues nothing.
5      The test sends SIGCONT to mesh2.compute.2's process.
6      mesh2.compute.2's process runs again; its digest reaches mesh2.admin.1.
7      mesh2.admin.1 tickles mesh2.compute.2: reached { Current }. mesh2.admin.1 clears os-stopped.
8      mesh2.admin.1 re-issues node.start to mesh2.compute.2 (node-start.md), and mesh2.compute.2 executes it:
8.1      mesh2.compute.2 runs node.mesh.join on mesh2.admin.1 and emits node.joined.
8.2      mesh2.compute.2 runs hydrate_before_ready.
8.3      mesh2.compute.2 obtains complete topology (node.topology.get), any missing Build floor, and rafka-time last.
8.4      mesh2.compute.2 emits node.started.
8.5      mesh2.admin.1 takes node.started, sets mesh2.compute.2 Ready through the node status apply and gossips it.
8.6      node.started of the same (node.id, incarnation) ends NodeRestarting for every peer (gossip.md, node-stopped lifecycle overlay).
8.7      mesh2.admin.1 emits node.restarted on the restart's stream.
```

## Defects in the same trace that this proposal does not change

Each is its own PRD row. Luke's statements of 2026-10-15 are quoted.

| # | code | Luke 2026-10-15 | docs that differ today |
|---|---|---|---|
| D1 | mesh1.admin.1 opens a Build attempt for `node.restart` and dispatches `build.attempt.run` | "builds are only created if there is a topology change (nodes coming or leaving)" | node-rpc-envelope.md typed objects (`node.restart`: build.create 0x20 carries the workflow); build.md §4.1 (a restart is an attempt of the accepted Build) |
| D2 | the executor sets the node's status before the node received a command: pipeline.rs:2249 (`Leaving`, `sink.publish`), runner.rs:151 (member status `Leaving`), runner.rs:148 (a `NodeLeaving` frame, which node-mesh-leave.md keeps reserved and unpublished) | the restart "sends the commands that the node itself executes: connection.delete (all), mesh.leave, node.stop"; the node tells the executor its status and the executor gossips it | none: states.md (who sets each state) and node-mesh-leave.md already say this |
| D3 | control_plane.rs:922 refuses the next command `node-not-live`, from the status D2 set | the node "just hangs out waiting for the next command" | `node-not-live` is in no doc |
| D4 | after a failed attempt the executor does nothing more | the executor runs node.tickle and, "once it replies", re-issues node.start | node-restart.md has no failure branch |

## Doc changes on acceptance

- [node-runtime-inspect.md](../../architecture/lifecycles/node-runtime-inspect.md): the `os-stopped`
  result, its step in the numbered sequence, the span verdict; a NotSent command's executor among its
  callers.
- [deployment-provider.md](../../architecture/deployment-provider.md): `inspect` answers `OsStopped`
  for a process (state `T`) and a container (`paused`).
- [node-recovery.md](../../architecture/lifecycles/node-recovery.md) step 3: `os-stopped` records the
  fact and opens no hold; step 4 applies to `running` only.
- [node-tickle.md](../../architecture/lifecycles/node-tickle.md) "What follows": the executor of a
  NotSent command inspects on `unreached`.

## Code changes on acceptance

- `provider.rs:298`: `DeploymentStatus::OsStopped`; every match on `DeploymentStatus` (16 uses of
  `DeploymentStatus::Running` in rdm-admin and rdm-node) handles it, including container.rs
  `signal_stop`, which signals only a `Running` runtime.
- `process.rs:127`/`:443`: the state letter; `container.rs:621`: `paused` → `OsStopped`.
- The executor's NotSent path runs `node.tickle`, then `node.runtime.inspect` on `unreached`.
- Node recovery: `OsStopped` records `os-stopped` and opens no hold.
- Functional cells: the process provider on a child it SIGSTOPs (`os-stopped`) and SIGCONTs
  (`running`); the container provider on `docker pause` / `docker unpause`; C17 to the sequence above.

## Open questions

- Q1: a runtime that stays `os-stopped`. Under this proposal nothing terminates it, and it stays
  surfaced on `node.get` until it runs again or a person deletes the node. Recommendation: keep it so;
  whoever sent the signal owns the runtime until it is continued.
- Q2: the fact's name, `os-stopped`.
