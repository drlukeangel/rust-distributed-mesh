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

- **A: `admit_attempt` on every node, ordered by the Build's acceptance stamp** (amended by Architect 1's
  review, 2026-10-25):
  - **What is carried.** The state-sync handoff and every application operation it sends carry
    `build_id`, `submitted_at_ms` and `attempt`. The order is `(submitted_at_ms, attempt)`; a Build id is
    never ordered. `submitted_at_ms` is the acceptance stamp, strictly increasing across accepted
    Builds by construction: `max(now, newest accepted + 1)`, the newest read from the builds store
    (`control_plane.rs:582`, R-BI3), so it holds across a restart and a clock rollback. A newer
    Build's attempt 1 supersedes an older Build's attempt 8.
  - **What RDM does.** `admit_attempt(build_id, submitted_at_ms, attempt)` compares against the node's
    durable admitted triple, fed by the requests themselves and by the fabric state the node hears. An
    older pair is refused by name, `superseded`; a different `build_id` carrying the same
    `submitted_at_ms` is refused by name, `ambiguous-order`; an equal triple is admitted (a repeat).
    A newer triple is written durably beside the node's row before `admit_attempt` returns, so its
    effect is never allowed ahead of its record.
  - **What the application does.** It runs `admit_attempt` and the mutation under the same
    per-resource serialization every competing attempt takes. Work prepared outside it (an async
    restore) takes the serialization again and calls `admit_attempt` again immediately before
    publishing. Repeats of one attempt stay subject to the operation's own idempotency.
  - **Changes:** the handoff gains `submitted_at_ms`; one node API and one durable value; the
    `sync_state` section of fabric-node-hooks.md and the handoff fields on node-rpc-envelope.md
    "Application state-sync handoff" and fabric-state-sync.md, landed with the code. RDM owns the
    comparison and the durable value; the application owns applying it at each mutation and
    publication.
  - **Cells:** a delayed old restore after a newer seed is refused and the seed stands; no write
    interleaves between the final check and the mutation; a restart keeps the admitted triple; a newer
    Build's attempt 1 supersedes an older Build's attempt 8; equal stamps with different Build ids are
    refused; a clock rollback does not lower the order.
- **B: a node reads the attempt it holds** (141's arm a): an accessor over the state the node has
  heard. Changes: one read accessor. Leaves the race of §4.
- **C: RDM holds attempt N+1 until attempt N's calls end** (141's arm b). Changes: the fabric-primary's
  state-sync step; RDM cannot see the application's calls end.
- **X: something else.**

**Recommendation: A as amended.** It is the standard monotonic-token check a stale holder's writes are
refused by, ordered by a durable acceptance stamp RDM already mints (never by Build id); it closes the
race B leaves; the application adds one call under its own per-resource serialization and no ordering
logic of its own.
