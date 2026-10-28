## Q129: a non-admin primary commanded to move elects its successor from its own view

### 1. Spec
`lifecycles/node-primary-update.md` :12-13: "Who is primary is the election's result, computed by every
node-admin from the facts it holds." :30 (the primary): "Makes the `node.primary.update` call on its
successor's exact birth". :31: "For any other type the mesh-primary is commander and writer, and the primary
only makes the call". :47-49 step 2: "The writer pre-checks from its own view: the election over the nodes of
that type in the mesh with the primary excluded names a Ready successor. If it names none, the step
is refused by name". :53 step 4: "The primary calls `node.primary.update` on the successor's exact birth."
`node-rpc-envelope.md` :1543-1545: "the fabric-primary commands a mesh-primary's update with
`UpdatePrimary { node_type, build_id, run, operation }` on the primary, and the mesh-primary commands another
type's primary the same way." Owner: the mesh-primary names the successor (writer, step 2); the primary calls it.
Sequence: pre-check names successor → command → primary calls that successor → `node.primary.updated`.

### 2. Issue
`node.stop` of `mesh1.rpc.1` fails `cannot transfer: no secondary of type rpc-node in mesh mesh1` 90 ms after
`mesh1.rpc.2` was applied Ready at the mesh-primary; 1 in 1 land-gate runs at 1918053cb
(first sighting; green on every earlier gate); `memory_estate::a_node_admin_on_the_memory_backend_runs_the_node_lifecycle_and_writes_nothing_to_the_file_store`.

### 3. Flame
Spans: `/home/admin/rust-distributed-mesh-deploy/kept/gate-gates-e6s8-1918053cb-rdm-test:memory_estate/testkit.2957211-4026531836.spans.jsonl`
(cells export no OTEL; the rpc node's own spans were not kept).

CONTRADICTION: the writer's pre-check refused nothing (zero `rdm.node_admin.primary.reject.via-primary-update`
spans, `side=commander reason=no-successor`, in the run), yet the command ended on the no-successor text.

```
+1.064 rdm.mesh.membership.update.from-node-started {"member": "mesh1.rpc.2", "node": "mesh1.admin.1", "node_id": "napv7z4r5wwk", "prior": "Pending", "status": "Ready"}
+1.064 rdm.node_admin.status.update.via-declaration {"node": "mesh1.admin.1", "op": "declare-node-state", "outcome": "applied", "receiver_is_primary": "true", "sender": "mesh1.rpc.2", "state": "Ready"}
+1.204 rdm.node_admin.primary.update.via-command-member {"build_id": "xaxpcw45ghb7", "detail": "Failed { step: \"primary-update\", reason: \"cannot transfer: no secondary of type rpc-node in mesh mesh1\", code: FailedPrecondition }", "node": "mesh1.admin.1", "node_type": "rpc-node", "operation": "node.primary.update:xaxpcw45ghb7:2:node.stop:mesh1.rpc.1", "outcome": "failed", "primary": "mesh1.rpc.1", "run": "2"}
+1.204 rdm.node_admin.build.update.via-dispatch {"attempt": "2", "build_id": "bid_xaxp-cw45-ghb7", "executor": "mesh1.admin.1", "outcome": "failed", "reason": "cannot transfer: no secondary of type rpc-node in mesh mesh1"}
```

### 4. Observations (commit 1918053cb)
- [code] `crates/rdm-admin/src/fabric_handover.rs:684-689`: the mesh-primary's pre-check elects over its own view (`commanded_cohort_successor`) and refuses with `side=commander reason=no-successor` when none; on success it discards the successor it found.
- [code] `crates/rdm-admin/src/fabric_handover.rs:690`: the command it sends is `UpdatePrimary { node_type, build_id, run, operation }`: no successor.
- [code] `crates/rdm-node/src/member_handover.rs:233-238`: the rpc primary runs "Step 2" itself, an election over its own gossip membership book, and answers `Failed { primary-update, no_secondary(..), FailedPrecondition }` when its book holds no Ready node of its type.
- [flame] the pre-check passed at the mesh-primary (no commander refusal span); the refusal is the primary's (`via-command-member` carries the primary's `Failed`).
- [code] `crates/rdm-client/src/calls.rs:488-495`: `node.primary.update` from a caller sends the same `UpdatePrimary` to any path; the method's text names the mesh-primary as the one commanded.
#### Inferred
- [inferred] `mesh1.rpc.1`'s book had not yet taken `mesh1.rpc.2`'s Ready digest 140 ms after the mesh-primary applied it — from the timeline and by elimination (the pre-check passed on the same fact).

### 5. Delta
Spec says the writer (mesh-primary) names the successor and the primary calls it; code has the non-admin primary run its own election over its own view.

### 6. Arms
Every arm assumes the writer's step-2 pre-check stays the one election a non-admin primary update runs on.
- A: `UpdatePrimary` carries `successor: Option<Birth>`; the mesh-primary sends the birth its pre-check named, the primary calls it without electing; `None` at a non-admin primary is refused by name; the fabric-primary's command to a node-admin and a caller's command send `None` (a node-admin elects) — changes wire (one field, fixture `handover_wire.rs:55`) and the primary's step (`member_handover.rs:233-238`).
- B: no code: the page states the primary elects from its own view and refuses on it — changes the page only (`node-primary-update.md` step 4, `node-rpc-envelope.md` :1543-1545); a commanded stop can fail by name while a secondary is Ready at the writer, and the caller resubmits.
- x: the delta itself is the fix: the successor is named once, at the writer — changes the primary's tier (it stops electing).

### Page text for arm A (exact)
`node-rpc-envelope.md` :1543-1545, old:
> The commanded arm rides the same op: the fabric-primary commands a mesh-primary's update with
> `UpdatePrimary { node_type, build_id, run, operation }` on the primary, and the mesh-primary commands another type's
> primary the same way.

new:
> The commanded arm rides the same op: the fabric-primary commands a mesh-primary's update with
> `UpdatePrimary { node_type, build_id, run, operation, successor }` on the primary, `successor` none: a node-admin
> computes the election itself. The mesh-primary commands another type's primary the same way, `successor` the exact
> birth its pre-check named (step 2), and that primary calls it without an election of its own; commanded with
> none, it is refused by name.

`lifecycles/node-primary-update.md` :53, old:
> 4. The primary calls `node.primary.update` on the successor's exact birth.

new:
> 4. The primary calls `node.primary.update` on the successor's exact birth: for a type other than `node-admin`,
>    the birth the mesh-primary's command names (step 2).
