# Q171: the fabric goes from pending to ready-for-traffic

Luke, 2026-10-26: state-sync and state-commit are removed. Q171.1 (the `fabric-not-ready` refusal
goes with the scratchpad) and the gossip change are approved with it. The node's `before_ready` hook
stays: it is where a node gets its projections from node-admin.

## Why

- **State-commit wrote again what is already written.** The scratchpad held RDM's rows in memory
  until state-commit wrote them to the store. The store root writes to its files from Day 0
  (`FileBackend::open`, `crates/rdm-admin/src/storage_backend.rs`). The mesh-primary and the
  fabric-primary already write the Node, Mesh and Fabric rows at each birth, so the commit-state
  action only wrote the same topology a second time.
- **State-sync is Rafka's, and Rafka runs it itself.** RDM has no `sync_state` hook in code.
  Rafka's node-admin runs its Day-0 work and reports it itself (`report_state_synced`,
  rafka-v2 `node-admin/src/lib.rs:12691`).

## The steps

- **R-S1. The fabric's states are `pending` → `ready-for-traffic`.** `FabricState` keeps `Pending`
  and `ReadyForTraffic`; `StateSync` and `StateCommit` are deleted, with no compatibility read: a
  stored row naming either value is from a fabric that no longer runs. When every mesh the accepted
  Build names is ready-for-traffic, the fabric-primary runs the open-traffic check-ins
  (`OpenTraffic` / `TrafficOpened`, unchanged), sets ready-for-traffic and sends the one
  `traffic_opened` notice (unchanged). A resize or an executable replacement still puts the fabric
  back to pending; its Build completes when the fabric is ready-for-traffic again.
- **R-S2. The `sync_state` hook goes.** The `SyncState` / `StateSynced` handoff, its operation
  `state-sync:<fabric_id>`, the `no-state-synced`, `state-sync-failed` and `state-synced-refused`
  blockers and their spans are deleted. RDM carries no Day-0 application work. Rafka runs its Day 0
  in its own code. It opens client traffic on the `traffic_opened` notice.
- **R-S3. The scratchpad goes.** Every writer writes its rows to the store root from Day 0. The
  `CommitState` / `StateCommitted` pair, operation `commit-state:<fabric_id>`, the
  `StateCommitting` / `StateCommitted` frames, `StateActions::commit_state`, `StorePublication` and
  the root's scratchpad and `writes_to` are deleted. `StateActions` keeps `open_traffic`.
- **R-S4 (Q171.1). The `fabric-not-ready` refusal goes.** Its only reason, on the page and in the
  code, was that before state-commit the rows lived only in their writers' scratchpads. While the
  Day-0 Build runs, every other Build is still refused `build-in-progress` (Q170). A property command
  is admitted at any time, as `node.config.update` is (Q166).
- **R-S5. Gossip (approved).** `FabricStatus` loses its `state-sync` and `state-commit` values; the
  frame catalog loses `StateCommitting` and `StateCommitted`. A Day-0 flame is captured before and
  after the change: pending → ready-for-traffic with the open-traffic check-ins.
- **R-S6. Pages.** `lifecycles/fabric-state-sync.md` and `lifecycles/fabric-state-commit.md` are
  deleted. Every statement of the two states, the scratchpad, `sync_state` and `fabric-not-ready`
  is removed from `states.md`, `fabric-create.md`, `fabric-ready-for-traffic.md`, `mesh-create.md`,
  `fabric-node-hooks.md`, `build.md` §10 and §11, `fabric-primary.md`, `gossip.md`,
  `node-rpc-envelope.md`, `node-identity-and-state.md`, `node-admin-lifecycle.md`, `telemetry.md`
  and the lifecycle index, in the same landing as the code.
- **R-S7. Rafka.** 141 is told the landing SHA: there is no state-sync hook and no `state-sync` or
  `state-commit` status; Rafka's Day 0 is its own and opens traffic on `traffic_opened`.

Q168 (an obsolete state-sync attempt's work) was withdrawn; with no state-sync it has no subject.
