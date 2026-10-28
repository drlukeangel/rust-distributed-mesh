# Q174 and Q175: the open-traffic checklist goes; the ready hooks

Luke, 2026-10-26. Q174: the open-traffic checklist is deleted, whole: "you create/start a node you
dont tell it to open traffic". Q175 (locked): the application's hooks are `pre_node_ready`,
`on_node_ready`, `on_mesh_ready` and `on_fabric_ready`. Q176 (the state itself named `ready`) is asked
separately.

## Why

A node is ready when its `before_ready` returns `Ok` and it reports `node.started`
(fabric-node-hooks.md, "Outcomes and the gate"). A mesh is ready when every planned birth
is; the fabric when every mesh is. The open-traffic checklist then sent `OpenTraffic` to every node of
every mesh, each node ran `StateActions::open_traffic`, bound to `NoAction` in RDM, the demo, rafka-v2
main and every 141 lane, and checked in `TrafficOpened`. It commanded nothing and duplicated the fact
the fabric-primary already held at `rdm.node_admin.fabric.update.via-every-mesh-ready`.

`after_ready` was spawned at `node_start.rs:488` and registered by nothing; `traffic_opened` was
registered by RDM's own test app and the demo test only. "for-traffic" told ready apart
from a node state `ready` that no longer exists. `pre_` names a hook that is asked first and holds the
act until it answers (git's `pre-commit`); `on_` names a notice of what happened (tokio's
`on_thread_start`, tower-http's `on_request`).

## Steps

- **R-RH1. The open-traffic checklist is removed.** `StatusRequest::OpenTraffic` and
  `StatusRequest::TrafficOpened` become `Reserved14` and `Reserved15`, refused by name; the gossip
  `Frame::TrafficOpening` and `Frame::TrafficOpened` positions and the CloudEvents types
  `rdm.fabric.traffic.opening` and `rdm.fabric.traffic.opened` are reserved, refused before decode.
  `StateCommand`, `StateActions`, `NoAction`, `run_action`, the mesh and fabric checklists of the
  open-traffic operation, the member's check-in and the `open-traffic:<fabric_id>` operation go. The
  checklist of an adopted status (R-S3, `ProbeNodeState`) stays.
- **R-RH2. The fabric is ready when every mesh is.** When every mesh the accepted Build names is
  ready and its attempt is claimed, the fabric-primary puts its ready row, enters
  ready and calls `on_fabric_ready`. A resize or an executable replacement still puts the
  fabric back to pending.
- **R-RH3. The hooks.** `NodeHooks { pre_node_ready, on_node_ready }` replaces `BeforeReadyHook`;
  `FabricHooks { on_mesh_ready, on_fabric_ready }`. `pre_node_ready` blocks as `before_ready` did
  (`Ok`, `Blocked { reason, retry_on }`, `Failed { reason }`; no timer). The three `on_` hooks are fire
  and forget, each called once per entry to the state by the one that set it: the node after
  `node.started`, the mesh-primary after its mesh's ready is written on its Mesh record
  (Q144), the fabric-primary after its ready row. A successor that adopts a state calls nothing.
  Contexts: `NodeReadyCtx` (today's `BeforeReadyCtx`), `MeshReadyCtx { fabric_id, mesh_id, mesh }`,
  `FabricReadyCtx { fabric_id, build_id, attempt, context }`.
- **R-RH4. Spans.** `via-before-ready-attempt` → `via-pre-node-ready-attempt`,
  `via-before-ready-blocked` → `via-pre-node-ready-blocked`, `via-after-ready` → `via-on-node-ready`,
  `rdm.node_admin.fabric.update.via-traffic-opened-hook` → `rdm.node_admin.fabric.update.via-on-fabric-ready`,
  new `rdm.node_admin.mesh.update.via-on-mesh-ready`.
- **R-RH5. Pages.** fabric-ready.md loses the command-pair block and the check-in
  sequence; states.md loses the open-traffic check-ins (its intro, the "Who sets" fabric row, the
  "Setting a state" ready row, the traffic-opened bullet); fabric-node-hooks.md, gossip.md,
  node-rpc-envelope.md, telemetry.md, node-start.md, the lifecycle index, build.md and the consumer
  guide name the four hooks.
