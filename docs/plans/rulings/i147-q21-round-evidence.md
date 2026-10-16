# Q21 — What round evidence may a passive hook receiver install before reconciliation?

## 1. Spec

Owner: the fabric-primary owns fabric progression; each primary owns its directed round checklist;
ordinary nodes receive hooks on their mesh channel.

- `docs/architecture/gossip.md:324–334`, Fabric round gossip hooks: “Identity is
  `(kind, fabric_id, build_id, attempt, operation, node_id, incarnation)`”; “Neither timestamp nor
  forwarded receipt supplies liveness or operation authority”; “Gossip receipt cannot replace
  the authenticated directed completion”; “Lost events are repaired through the held
  round/accepted Build evidence; snapshot installation preserves the accepted round facts”.
- `docs/architecture/gossip.md:526`, receive-table hook row: “reconciled against the round's
  completion; a hook after the round moved on reopens nothing”.
- `docs/architecture/lifecycles/states.md:157–168`: the checklist names the accepted Build's
  planned births; only matching check-ins complete it; a successor primary resends the current round.
- `docs/architecture/lifecycles/states.md:172–178`: “A resize or roll puts the fabric back to
  pending”; the internal commit-state and open-traffic rounds run again.

Expected sequence: accepted round command → matching directed completion → fabric progression;
a delayed hook cannot reopen that round. An unrelated newer frame cannot discard a required hook.
The user's current instruction adds the live cell for a later round's applied command for the same
birth or fabric progression past the round. It also requires one landing for all four hooks.

## 2. Issue

All four hook repeat cells apply twice (4/4 live cells); passive membership has no installed round
or accepted Build evidence with which to bind a held fabric state to the hook's exact round.

## 3. Flame

### Observations

RED commit `3dc216a5`, live two-endpoint mesh, stem `operation_facts`: 6 passed, 8 failed,
0 ignored, 0.07 s (build 22.6 s). Four failures prove repeat application. Four are diagnostic
fabric-status probes whose same-round completion premise cannot be established by their inputs;
they are **not** acceptance proof of a reopened completed round. No birth notification changed.

The first and repeated TrafficOpened produce the same ordinary apply span, without a duplicate
outcome (the test changes only the evidence timestamp to pass transport byte dedup):

[First](http://localhost:16686/trace/320c3d9a88ac6cfb16f586d6aabd7af2),
[repeat](http://localhost:16686/trace/40edd587ba0bf9ac53c0097652cb84d7).
Both traces were retrieved successfully from the live Jaeger API.

```text
rdm.mesh.membership.update.via-round-frame kind="traffic-opened" fabric_id="qvf7x9gcf43w" build_id="held-round-build" attempt="1" operation="open-traffic:qvf7x9gcf43w" subject_id="14q3h8m1hv38" incarnation_id="e0btr28qhxrr" forwarded_by="" via="mesh-channel"
rdm.mesh.membership.update.via-round-frame kind="traffic-opened" fabric_id="qvf7x9gcf43w" build_id="held-round-build" attempt="1" operation="open-traffic:qvf7x9gcf43w" subject_id="14q3h8m1hv38" incarnation_id="e0btr28qhxrr" forwarded_by="" via="mesh-channel"
```

[Fabric-status probe](http://localhost:16686/trace/d2777d8fbf1382a10297bc33433f54d4)
then [hook receipt](http://localhost:16686/trace/d27ee7047bd0b2b5366d8826fbf5b56d):

```text
rdm.mesh.membership.update.via-fabric-status fabric="r6zeqevz3kns" status="ready-for-traffic" publisher="mesh1.admin.1" changed_at_rafka_ms="10000" taken="true"
```

The status has no round identity. Its receipt before the hook proves neither that it completed
that hook's round nor that it belongs to an earlier Build. The timestamp cannot resolve this.

## 4. Observations (commit 3dc216a5)

- [code] `crates/rdm-transport/src/membership.rs:2091–2120`: every hook emits the ordinary
  `via-round-frame` span; an unforwarded completion heard on the mesh also invokes the carrier.
  No identity dedupe or round reconciliation runs here.
- [flame] The two TrafficOpened traces above contain ordinary apply spans for the identical
  full operation identity; they contain no via-duplicate span.
- [code] `crates/rdm-transport/src/membership.rs:462–472`: FabricStatus carries fabric, status,
  publisher, forwarding metadata and change timestamp; neither Build, attempt nor round.
- [code] `crates/rdm-transport/src/snapshot.rs:54–69,284–305`: installed topology contains
  digests, lifecycle overlays and retained deletions; no round facts or accepted Build association.
- [code] `crates/rdm-node/src/node_self.rs:609–623,652–660`: accepted/completed member actions
  live in NodeSelf's private rounds_seen, separate from membership receipt.
- [code] `crates/rdm-admin/src/fabric_state.rs:185–217`: primary checklists are local RoundBook
  state; closing a round removes it.
- [code] `crates/rdm-admin/src/fabric_rounds.rs:36–85`: admin supersession reads the accepted
  Build and, across Builds, the hook round's Build. A round ahead of the local Build is not
  superseded; an unreadable Build leaves the answer unknown and names the read failure.
- [code] `crates/rdm-test/tests/gossip/operation_facts.rs:130–141,180–182`: the fabric probe
  installs only a status, then assumes that it completed a synthetic Build. It must gain actual
  accepted/completed round evidence before it can be an acceptance cell.

### Inferred

- [inferred] A blanket ready-for-traffic check would also suppress a hook from a subsequent Build
  whose pending status this receiver has not heard. Fabric cycles and best-effort delivery permit
  that sequence; the frames alone cannot distinguish it from a delayed completed-round hook.
- [inferred] Dedupe is implementable from current fields, but it cannot supply the missing
  authoritative relationship. Treating a gossip completion as a directed completion would violate
  the quoted contract. The later-command cell needs the actual command application handoff too.

## 5. Delta

Spec says accepted round facts are carried by snapshot installation and held round/accepted Build repair; code has no such path to the passive membership receiver.

## 6. Arms

Every arm assumes hook identity stays `(kind, fabric_id, build_id, attempt, operation, node_id,
incarnation)` and neither hook timestamps nor forwarding metadata establishes authority.

- **A (recommended):** expose the authoritative round/accepted Build facts to the receive framework,
  locally from the command/completion paths and remotely through the existing topology-read/snapshot
  repair path; unknown remains unknown until repaired. Changes: wire (typed round evidence on the
  repair result, exact shape reviewed before reseal), publisher and receiver wiring; no timer or
  hook-envelope change. The same held evidence supplies duplicate and completion reconciliation.
- **B:** approve a narrower e2 row: dedupe all receivers now, reconcile only where authoritative
  local evidence already exists, defer passive repair to e3. Changes: acceptance scope and e2/e3
  sequencing; requires an explicit change to the current instruction.
- **x:** specify another evidence path or a different reading of the completion rule.

No receive behavior, architecture text or wire format has changed. R10 remains open. No GREEN
or landing exists; the four hooks remain one pending landing. Tests and this packet are retained
in lane e7s3, branch codex/i147-e2-round-hooks. No other initiative's cell was edited or repaired.
