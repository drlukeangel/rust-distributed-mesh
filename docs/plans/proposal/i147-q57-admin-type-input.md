# Q57 — node-admin's Wiring type input and its initially held channels

Asked 2026-10-17. e5 read only; no app-channel implementation or RED is claimed. s88/Q53/Q54 landed `8bd6681d` through the land script (concurrent gate GREEN, 211 s).

## 1. Spec

[gossip.md §8](../../architecture/gossip.md#8-entity-projection-gossip-remains-projection-not-source-of-truth): `types` is “empty for a node-admin, which declares nothing of its own”; its `held` is “the union of its members' declarations and what it learned from other node-admins”. Its mesh's node-admin joins new member types **before** answering JoinNode. Owner: the node-admin; the application subscribes through Channel and owns projection rows.

[i147 PRD e5, arch review 3 item 5](../i147-gossip-conformance-prd.md): “node-admin takes its declared types and ALPNs through `Wiring`, beside `hydration` and `serve_app`; no second registration surface.” The application joins its channel before its snapshot pull. The admin's application hydration runs before Ready. These statements do not say whether Wiring's input may seed a channel before any member or peer has declared that type.

## 2. Issue

The e5 implementation read finds two different possible admissions for a node-admin configured with Wiring types before its first member joins: start those subscriptions immediately, or require its own input to be empty and admit only learned held types; this is a design ambiguity, not an observed app-event failure.

## 3. Flame

### Observations

[Baseline node-admin boot](http://localhost:16686/trace/7d7170819fc0b7e2a1ebde0a023c1b24), current live container controls, before adding app channels. The node-admin joins its own mesh with zero seeds. Verbatim exported span records:

```json
{"attributes":{"busy_ns":"215711","code.filepath":"crates/rdm-admin/src/admin/boot.rs","code.lineno":"691","code.namespace":"rdm_admin::admin::boot","idle_ns":"13088","mesh":"mesh1","mesh_id":"5gttxfs17wgt","node":"mesh1.admin.1","node_id":"qv0sexzj6x0b","thread.id":"1","thread.name":"main","which":"own"},"end_unix_nano":1792289320304072146,"events":[{"attributes":{"code.filepath":"crates/rdm-admin/src/admin/boot.rs","code.lineno":"700","code.namespace":"rdm_admin::admin::boot","level":"INFO","target":"rdm_admin::admin::boot"},"name":"joined the mesh channel","time_unix_nano":1792289320304063810}],"name":"rdm.node.mesh.update.via-joined","parent_span_id":"ad0713520105619f","service":"rafka-node-admin","span_id":"69be3439586a7335","start_unix_nano":1792289320303846211,"trace_id":"7d7170819fc0b7e2a1ebde0a023c1b24"}
{"attributes":{"busy_ns":"0","code.filepath":"crates/rdm-node/src/workflows/node_start.rs","code.lineno":"351","code.namespace":"rdm_node::workflows::node_start","idle_ns":"0","mesh_id":"5gttxfs17wgt","node":"mesh1.admin.1","seeds":"0","thread.id":"1","thread.name":"main"},"end_unix_nano":1792289320304130034,"events":[],"name":"rdm.mesh.node.update.via-membership-joined","parent_span_id":"d9ac68c49d1ad385","service":"rafka-node-admin","span_id":"ad0713520105619f","start_unix_nano":1792289320302258219,"trace_id":"7d7170819fc0b7e2a1ebde0a023c1b24"}
{"attributes":{"busy_ns":"171872","code.filepath":"crates/rdm-admin/src/admin/boot.rs","code.lineno":"734","code.namespace":"rdm_admin::admin::boot","idle_ns":"13136","mesh":"mesh1","mesh_id":"5gttxfs17wgt","node":"mesh1.admin.1","node_id":"qv0sexzj6x0b","thread.id":"1","thread.name":"main","which":"backbone"},"end_unix_nano":1792289320304814831,"events":[{"attributes":{"code.filepath":"crates/rdm-admin/src/admin/boot.rs","code.lineno":"743","code.namespace":"rdm_admin::admin::boot","level":"INFO","target":"rdm_admin::admin::boot"},"name":"joined the backbone channel","time_unix_nano":1792289320304807711}],"name":"rdm.node.mesh.update.via-joined","parent_span_id":"d9ac68c49d1ad385","service":"rafka-node-admin","span_id":"6caed43acb95cf3d","start_unix_nano":1792289320304631935,"trace_id":"7d7170819fc0b7e2a1ebde0a023c1b24"}
```

This is baseline lifecycle evidence, **not** an app-subscription flame. No app channel exists on this source, so it cannot show either proposed admission behavior. Imported and queried nonempty evidence: `/home/admin/rust-distributed-mesh-deploy/kept/i147-e5-admin-boot-read/`; verification `i147-e5-admin-boot-flame.log` in the same deploy root. The original source of the live controls was `0d7e46af` (rebased documentation equivalent `5ff248a9`); runtime and span line positions here are the original exported source.

## 4. Observations (commit d4b59d2f, on landed 8bd6681d)

- [code] `docs/architecture/gossip.md:854-858`: node-admin's own advertised types is empty; the listed inputs of held are members and other admins.
- [code] `docs/plans/i147-gossip-conformance-prd.md:524-525`: the node-admin type input is supplied through Wiring.
- [code] `crates/rdm-admin/src/wiring.rs:148-186`: Wiring already carries hydration and served app ops; it has no type input yet.
- [code] `crates/rdm-admin/src/admin/boot.rs:1506-1509`: the admin's app hydration runs after joining the mesh and before its readiness gate.
- [code] `crates/rdm-core/src/join.rs:38-43`: current JoinNode carries only a digest; adding the settled member type declaration remains e5 work.
- [flame] The baseline start's mesh-membership join has `node=mesh1.admin.1`, `seeds=0`; it is a real node-admin start, not an app-channel test.

### Inferred

- A nonempty Wiring subscription input that immediately joins channels would add an initial source of held beyond the page's two listed sources. Treating Wiring's own list as necessarily empty keeps the page but narrows the PRD's input. Neither meaning is implemented or represented as approved here.

## 5. Delta

Spec says node-admin's held types come from members and peers; code has no path for the PRD's own Wiring type input to become an initial subscription before either source supplies it.

## 6. Arms

Every arm keeps node-admin's published `types` empty, uses one Wiring input surface, keeps application ordering outside RDM, and changes no timer or bound.

- **A:** Wiring's type input is the admin application's initial subscriptions; join them before its hydration hook and seed held from them, then grow held from members and peers. Wire: the already planned held/hash fields, with this initial set included; publisher: admin's held hash; tier: node-admin; fork: none. The two exact page replacements below land with e5.
- **B:** Wiring's own type input is empty for a node-admin; its app may join only a type already learned into held from a member or peer. Wire and held/hash ownership: the settled e5 plan; publisher/timer/tier/fork: unchanged. Clarify the PRD item to say Wiring supplies an empty own declaration on an admin, and the app attaches subscriptions as held types become known; the page's two sources of held stand.

### Arm A page lines — exact old → new, proposed only

`gossip.md §8`, sets paragraph fragment:

Old:

```text
Two sets, both answered by `node.get`: `types`, what the birth declared, immutable
for its life (empty for a node-admin, which declares nothing of its own); and `held`, the types
the node has joined, equal to `types` for an ordinary node and, for a node-admin, the union of
its members' declarations and what it learned from other node-admins, growing as members join and
never shrinking within a birth.
```

New:

```text
Two sets, both answered by `node.get`: `types`, what the birth declared, immutable
for its life (empty for a node-admin, which declares nothing of its own); and `held`, the types
the node has joined, equal to `types` for an ordinary node and, for a node-admin, the union of
its application's initial subscriptions supplied through `Wiring`, its members' declarations and
what it learned from other node-admins, growing and never shrinking within a birth. The node-admin
joins its initial subscriptions before its application's hydration hook runs.
```

`gossip.md §8`, node-admin paragraph opening:

Old:

```text
**node-admin holds every channel, as the app.** node-admin holds the union of its members' declared
types (`held`), so every projected kind reaches it;
```

New:

```text
**node-admin holds every channel, as the app.** node-admin holds the union of its application's initial
subscriptions, its members' declared types and the types learned from other node-admins (`held`),
so every projected kind reaches it;
```

Corresponding PRD item: “node-admin takes its application's initial channel subscriptions and ALPNs through Wiring, beside hydration and serve_app; the subscriptions seed held, while the admin's own published types remains empty; no second registration surface.”
