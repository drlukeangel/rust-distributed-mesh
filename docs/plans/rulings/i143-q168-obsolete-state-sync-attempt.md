## Q168: how does a node refuse an obsolete state-sync attempt's work?

Asked by the lead, from Rafka 141's request (2026-10-25). One question, with a recommendation.

### 1. Spec

[fabric-state-sync.md](../../architecture/lifecycles/fabric-state-sync.md), "Application → fabric-primary":
"The hook's answer means the requested application work is complete for this state. … A foreign
Build, old attempt or incomplete application phase is not this completion." Owner: the fabric-primary.
Sequence: fabric-primary enters state-sync → calls `sync_state` with `(build_id, attempt)` → the
application does the work → its answer for the matching `(build_id, attempt)` starts state-commit.

[node-rpc-envelope.md](../../architecture/node-rpc-envelope.md), "Application state-sync handoff": the
handoff carries `build_id` and `attempt`; "RDM owns no day-0, seed or application writer logic."

No spec covers what a node does with work an older attempt's hook started on it.

### 2. Issue

Rafka's `sync_state` hook (on the fabric-primary node-admin) will call Rafka ops on other nodes:
`RestoreFrom { build_id, attempt, dir }` on each broker, `day-zero.create` / `seed.from_theme` on the
gateway. A client deadline ends the hook's wait, not the served work. When RDM starts attempt 2,
attempt 1's `RestoreFrom` can still be in flight and, landing after attempt 2's seed writes, erase
them. Not yet observed: Rafka's hook moves these ops onto the gateway in its next unit; the proof
cell is in Rafka's proposal (`docs/plans/proposals/i141-b3-day0-and-seed-on-gateway-ops.md`).

### 3. Flame

No flame: a design gap found by reading, before the ops exist. The proof is Rafka's cell: an
attempt-1 restore sent after attempt 2's seed must be refused, and the seed's rows still there.

### 4. Observations (commit `origin/main`, 2026-10-25)

- [code] `crates/rdm-node/src/fabric_states.rs:244-251`: `StateActions` receives only the node's
  `PathName`; no `StateKey` reaches the application.
- [code] `crates/rdm-node/src/node_self.rs:317`: `NodeSelf.states_seen` (the state keys this birth
  took) is private.
- [code] `crates/rdm-node/src/fabric_states.rs:205-218`: `ActedRecord::acted(&key)` answers only for a
  key already held; it orders nothing.
- [code] `crates/rdm-node/src/before_ready.rs:141`: `BeforeReadyCtx.attempt` is the hook's own retry
  count, not the Build attempt.
- [code] `crates/rdm-admin/src/fabric_status.rs:75` (`state_superseded_by`): the fabric-primary refuses
  an old attempt's answer; nothing refuses an old attempt's work on another node.

#### Inferred

- [inferred] A node reading only the attempt it has heard (by gossip) still races: attempt 2's state
  can reach a broker after attempt 1's late request does.
- [inferred] RDM cannot hold attempt N+1 until attempt N's calls end: those calls are the
  application's, made from its hook to other nodes; RDM does not see them end.

### 5. Delta

No spec covers this step; the code gives a node no way to compare a request's `(build_id, attempt)`
with the attempt the fabric now runs.

### 6. Arms

Every arm assumes the application's ops carry the `(build_id, attempt)` of the hook call that sent them.

- **A: `admit_attempt(build_id, attempt)` on every node.** A served op calls it before it changes
  anything. The node keeps a durable, never-decreasing high-water of the `(build_id, attempt)` it has
  seen, beside its row, fed by the requests themselves and by the fabric state it hears; an older
  pair is refused by name, `superseded`. Once attempt 2's first request reaches a node, attempt 1's
  late one is refused there, gossip or not. Changes: one node API and one durable value; no wire; the
  `sync_state` section of fabric-node-hooks.md states it.
- **B: a node reads the attempt it holds** (141's arm a): an accessor over the state the node has
  heard. Changes: one read accessor. Leaves the race of §4.
- **C: RDM holds attempt N+1 until attempt N's calls end** (141's arm b). Changes: the fabric-primary's
  state-sync step; RDM cannot see the application's calls end.
- **X: something else.**

**Recommendation: A.** It is the standard monotonic-token check a stale holder's writes are refused
by, it closes the race B leaves, and the application adds one call at the top of each served op and no
ordering logic of its own.
