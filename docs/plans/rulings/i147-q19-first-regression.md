# Q19 — MeshStatus inconsistency is e1’s first regression

**Ruling: Luke selected A, 2026-10-16.** Use the proven inconsistency regression for e1; fix it in the receive path and prove GREEN. No architecture or wire change.

## 1. Spec

`docs/architecture/gossip.md` §3.1, lines 504–506: “A frame equal in revision
to the held one but different in content is an inconsistency, refused by name, for every state
projection, seat record and snapshot publication.”

Line 522 gives MeshStatus replacement-state subject `(mesh | fabric, publisher)`, ordering
`changed_at_rafka_ms`, and identity `(subject, publisher, changed_at_rafka_ms)`.
Lines 534–537 name `rdm.mesh.membership.reject.via-inconsistent` (frame, subject, revision).
Owner: the receiving node. Sequence: hold a status, receive conflicting content at the same
revision, refuse it by name before applying anything.

`docs/plans/i147-gossip-conformance-prd.md` e1, lines 89–91, instead requires
`an_unkeyed_<frame>_delivered_after_a_newer_frame_about_its_subject_is_refused_stale`.
The demonstrated failure below is an inconsistency refusal, not a stale rollback. The contract
itself needs no change; this question concerns selection of e1’s acceptance cell.

## 2. Issue

On a live two-endpoint mesh channel in the test kit, conflicting MeshStatus content at the held
revision is silently ignored without the required inconsistency refusal; 1 failure in 1 run of
`receive_rules::an_unkeyed_mesh_status_with_equal_revision_and_different_content_is_refused_inconsistent`.

## 3. Flame

### Observations

- [Receipt 1](http://localhost:16686/trace/9d29876ac4d6d6d455aecd531c775469)
- [Receipt 2](http://localhost:16686/trace/f6fc35d858049ff6c75782276d66a969)

Verbatim captured receive spans (initial status, then conflicting status):

```json
{"attributes":{"births_held":"0","births_routable":"0","busy_ns":"29536","changed_at_rafka_ms":"100","code.filepath":"crates/rdm-transport/src/membership.rs","code.lineno":"1947","code.namespace":"rdm_transport::membership","forwarded_by":"","idle_ns":"24000","mesh":"mesh1","node":"mesh1.rpc.1","publisher":"mesh1.admin.1","status":"ready-for-traffic","taken":"true","thread.id":"5","thread.name":"tokio-rt-worker","via":"mesh-channel"},"end_unix_nano":1792185663951338810,"events":[{"attributes":{"code.filepath":"crates/rdm-transport/src/membership.rs","code.lineno":"1971","code.namespace":"rdm_transport::membership","level":"INFO","target":"rdm_transport::membership"},"name":"a mesh status is held: application routing selects the mesh's births only while it is ready-for-traffic","time_unix_nano":1792185663951334490}],"name":"rdm.mesh.membership.update.via-mesh-status","parent_span_id":"","service":"rdm-gossip-conformance","span_id":"73cf507d53005356","start_unix_nano":1792185663951292874,"trace_id":"9d29876ac4d6d6d455aecd531c775469"}
{"attributes":{"busy_ns":"7472","changed_at_rafka_ms":"100","code.filepath":"crates/rdm-transport/src/membership.rs","code.lineno":"1947","code.namespace":"rdm_transport::membership","forwarded_by":"","idle_ns":"9408","mesh":"mesh1","node":"mesh1.rpc.1","publisher":"mesh1.admin.1","status":"deleting","taken":"false","thread.id":"4","thread.name":"tokio-rt-worker","via":"mesh-channel"},"end_unix_nano":1792185663962084281,"events":[],"name":"rdm.mesh.membership.update.via-mesh-status","parent_span_id":"","service":"rdm-gossip-conformance","span_id":"9335d6aa51586726","start_unix_nano":1792185663962070025,"trace_id":"f6fc35d858049ff6c75782276d66a969"}
```

The held status remains ready-for-traffic; there is no rollback in this run. The assertion fails
because the conflicting receipt has no `via-inconsistent` refusal. Neither receipt claims that the
conflicting content was installed. The cell ran in 0.08 s; its build took 14.1 s.

## 4. Code

### Observations (commit 35af8ca1)

- [code] `crates/rdm-transport/src/membership.rs:898`: StatusBook passes only publisher and timestamp to its refusal check, omitting status content.
- [code] `crates/rdm-transport/src/membership.rs:920–927`: equal publisher/timestamp is refused as a repeat without comparing content; an older timestamp is already refused.
- [code] `crates/rdm-transport/src/membership.rs:1959–1966`: a refused MeshStatus records `taken=false` and returns without an inconsistency refusal span.
- [code] `crates/rdm-transport/src/receive_order.rs:145–166`: MeshStatus has no receive-framework ordering entry.
- [code] `crates/rdm-test/tests/gossip/receive_rules.rs:19`: the live cell sends each frame once over iroh-gossip, verifies the held status and then asserts the named refusal.
- [flame] The two linked receipts carry identical subject, publisher and timestamp, different statuses, and `taken=true` then `taken=false`.

## 5. Delta

Spec says equal-revision state projections with different content are refused by name; code treats equal publisher/timestamp as a repeat without comparing status content.

## 6. Arms

Every arm assumes the existing receive contract stands and R10’s missing publisher revision stays outside e1.

- **A (recommended):** use this proven MeshStatus inconsistency as e1’s first regression, replace its stale-only acceptance cell in the PRD, then fix classification in the receive path using the existing subject, publisher, timestamp and status content — changes the acceptance selection and receiver; no wire, publisher or timer change.
- **B:** keep e1’s stale-only acceptance selection and continue seeking a demonstrated stale rollback in its permitted unkeyed kinds; keep this RED cell owned by i147.e1 pending selection — changes the investigation fork, with no receive-path fix in this packet.
- **x:** another first-regression selection from Luke.
