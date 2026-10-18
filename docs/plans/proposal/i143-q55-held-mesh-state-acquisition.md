# Q55 — acquisition of held Mesh state after the pre-commit writer ends

Assigned to the i144 owner by Luke's Q54 A, 2026-10-17. This read accompanies that assignment; Codex does not widen Q54 to another held store. Q54's declared-state implementation and exact-birth controls are green. This is the subsequent live recovery failure, after healthy setup and the injected loss.

## 1. Spec

Q54 A makes only `status/<mesh>/declared` durable before Applied; other held rows keep their publication boundary. [Fabric authority §7](../../architecture/fabric-authority.md#7-mesh-primary-takeover-is-hydration-not-transition): “A successor mesh primary reads/hydrates the Mesh lifecycle state that already exists and adopts that exact state locally.” It adopts held or durable state, without manufacturing a transition. [Mesh investigation, a node-admin joins the live mesh](../../architecture/lifecycles/mesh-investigation.md#a-node-admin-joins-the-live-mesh), steps 2–4: maker topology/control context, local topology, completed own-mesh sweep, Ready, election and adoption of the mesh's held state. The fabric-primary creates the missing admin; the successor acquires its mesh's state.

## 2. Issue

In release `3cad5423`, `container_peer_mesh_lost_recovers_under_same_mesh_id` reaches healthy readiness and kills the exact two peer admins, then times out at estate.rs:405 after the unchanged 300 s recovery bound; its new admin remains Pending (one observed run).

## 3. Flame

Observations, verified in Jaeger from measured exported spans:

[Completed sweep](http://localhost:16686/trace/9b9ef36a06c96e2df2bf5f24d2a82aa9):

```text
rdm.node_admin.mesh.update.via-entry-sweep node="mesh2.admin.1" source="maker" mesh="mesh2" elapsed_ms="6016" pinged="4" reached="2" not_reached="2" local="mesh2.rpc.1" swept="mesh2.admin.1,mesh2.admin.2,mesh2.rpc.1,mesh2.rpc.2"
```

[Remaining blocker](http://localhost:16686/trace/b1291e06069d27cd81e2a5014c5abb51):

```text
rdm.node_admin.runtime.reject.via-not-authority-capable node="mesh2.admin.1" blocked="1" detail="mesh2.admin.1: the first admin of mesh2: its mesh's Pending has not been applied here by the fabric primary"
```

[Create attempt](http://localhost:16686/trace/c10d3ec4d050143feec7c20929ca09d0):

```text
rdm.node_admin.deployment.update.via-step node="mesh2.admin.1" build_id="bid_yy8e-xygc-ca06" attempt="3" step="ApplyMeshPending" outcome="complete" elapsed_ms="0" code="OK" detail="complete"
rdm.node_admin.deployment.update.via-step node="mesh2.admin.1" build_id="bid_yy8e-xygc-ca06" attempt="3" step="WaitForNodeReady" outcome="failed" elapsed_ms="30113" code="DEADLINE_EXCEEDED" detail="failed"
```

The new birth's [boot and topology](http://localhost:16686/trace/9b84a6451a0f0881b46e04ecc5e8e00f) are node.id `fjvkecp85cn8`, incarnation `521xp5y0247g`. `via-build-floor outcome="held" blockers=""` means its floor completed with facts already held; it is not the blocker. The initial eight births' declared-state puts are acknowledged with `holding=false`; the healthy setup succeeds.

Measured source JSONL, canonical cell log, OTLP and queried responses: `/home/admin/rust-distributed-mesh-deploy/kept/q55-mesh-state-read/`; import verification `/home/admin/rust-distributed-mesh-deploy/q55-mesh-state-flames.log`. Original release and its immutable common evidence are `s88-q54-release.log` and `kept/gate-gates-e7s3-3cad5423release/` in the same deploy directory.

## 4. Observations

Commit `3cad542319544c35571edb65d8328dda57f3eb06`:

- [code] `crates/rdm-admin/src/status_storage.rs:685`: `meshes` still opens through `root.records`; Q54 directly opens only `declared`. No second copy or format change exists.
- [code] `crates/rdm-admin/src/admin/boot.rs:1637`: Ready checks the locally applied mesh state and whether another admin of its cohort is live; `session.rs:67` otherwise blocks for Pending.
- [code] `crates/rdm-admin/src/admin/session.rs:635`: Build-floor `held` means no blocker remained. `:790` publishes sweep results; the sweep has finished in the flame.
- [code] `crates/rdm-admin/src/deployment/pipeline.rs:1784`: ApplyMeshPending succeeds with either the supplied hand-off, or `applied: null` when no hand-off was supplied. Its completion span alone does not prove an ApplyMeshState was sent.
- [code] `crates/rdm-admin/src/admin/runner.rs:626`: recovery chooses a hand-off from `first_admin_of` and `held_admins`; `:499` already has the mesh-state hand-off function. `topology.rs:166` and `executor.rs:164` define those two predicates.
- [flame] The joined birth completes its Build floor and local sweep, then names missing locally applied Mesh state as its remaining blocker. The create fails at readiness.

### Inferred

- Whether the hand-off was omitted, or the acquisition should have supplied the existing held Mesh state before this check, remains the i144 owner's read. The zero-duration completion span does not establish its receipt content. This is not evidence to make all held stores durable before state-commit.

## 5. Delta

Spec says the successor acquires the mesh's existing held lifecycle state and becomes Ready after its own-mesh sweep; flame shows the completed sweep followed by a continuing gate for locally applied Mesh Pending, without the acquired state.

## 6. Arms

Every arm keeps Q54 limited to declared-state durability, preserves the same MeshId and existing Mesh lifecycle state, and changes no timer, bound or wire format.

- **A:** The i144 owner traces the existing Mesh-state acquisition/hand-off and fixes that path so the recovered exact birth acquires the held state before its Ready check. Publisher/tier: the existing fabric-primary and successor; no synthetic lifecycle transition or second durable copy.
- **B:** If the existing contracts cannot provide that acquisition, the i144 owner brings the precise missing page line and its implementation delta to Luke before changing the phase boundary. Publisher/tier: selected by that packet; no broader durability exception is inferred here.

Ownership is already assigned to i144 by Q54; these are the follow-up read's boundaries, not a request for Codex to take that path. s88's live recovery proof is held on this read; i147 e5 follows s88's landing.
