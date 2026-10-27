# Span name census (rafka-v2 #3206, report)

Report only: no name, code or page is changed. Tree: branch `i143-3206-span-census` at `23bb09f97`.

Standard: `docs/eng/telemetry-patterns.md` §8, from `docs/architecture/telemetry.md:20`, `node-rpc-envelope.md` §Runtime spans, `node-identity-and-state.md` §9, rafka-v2 `docs/eng/observability-conventions.md`, and the vocabulary bans in `CLAUDE.md`. Checked per name: owner `rdm`; five segments; component a snake_case word listed in §8.1 (`node_admin`, `node_rpc`, `mesh`, `node`, `testkit`); one-word snake_case entity; verb on the twelve-word whitelist; reason kebab-case with `via-` or `from-`; no banned word in any segment; a literal.

## Method and cross-check

- Source of names: `docs/reference/rdm/spans.md` (578 names at 753 sites, scanner `tools/mesh-audit/src/telemetry.rs::emitters`, which reads the non-test source of `crates/`, `tools/`, `demo/` with `syn`, including span macros inside `macro_rules!` bodies). I ran a second, independent regex scan of every `*_span!` and `#[instrument(name = ..)]` under `crates/`, `tools/`, `demo/` and compared the literal names.
- Generated list against the code, names the code has that the list lacks (all in non-test source, all five segments, whitelisted verb, `via-` reason, no banned word, so conforming):
  - `rdm.node_admin.build.reject.via-superseded-attempt` (`crates/rdm-admin/src/build_drive.rs:1008`)
  - `rdm.node_admin.build.update.via-reconciling-stopped` (`crates/rdm-admin/src/build_drive.rs:743`)
  - `rdm.node_admin.fabric.update.via-atlas-receipt` (`crates/rdm-admin/src/fabric_atlas.rs:562`)
  - `rdm.node_admin.node.update.via-forward` (`crates/rdm-admin/src/node_config.rs:262`)
  - `rdm.node_admin.node.update.via-rows-held` (`crates/rdm-admin/src/admin/rows_writer.rs:153`)
  - `rdm.node_admin.node.update.via-storage-move-lost` (`crates/rdm-admin/src/storage_change.rs:371`)
  So `spans.md` is stale: it was regenerated before these landed (`c6d4b85e9`, `02db3214f`, `35214d22f` and others on 2026-10-27). Its line numbers are stale too (the `concat!` row says `control_plane.rs:1231`; the macro is at `:1254`). The table below uses current lines.
- Names the generated list has that the code no longer has: none. (Its row `rdm.node_admin.build.reject.via-` is the `concat!` stem, a finding below, not a stale row.)
- Test-only spans (inside `#[cfg(test)]` or `tests/`) are out of the census, as in the scanner: `capture.shared-callsite`, `iroh_actor`, `rg3_span`, `rdm.test.span.update.via-cell`, `rafka.*` and `rdm.demo.connections.proxy_to_direct` in `demo/tests/`.

## Totals

- Names checked: 584 (578 in `spans.md` plus the 6 above it lacks).
- Conforming: 522. Findings: 62 names.
- Findings by rule (a name can break more than one; none here does):

| rule | source | names |
|---|---|---|
| exactly five segments | telemetry.md:20; observability-conventions.md | 22 (21 with six segments, 1 with four) |
| no banned word | CLAUDE.md vocabulary | 19 (`seat` 10, `departed` 2, `retired` 2, `authority` 2, `fence` 1, `fenced` 1, `partition` 1) |
| verb on the whitelist | node-identity-and-state.md §9 rule 2 | 17 (`get` 9, `created`/`deleted`/`joined`/`started`/`stopped`/`updated` 6, `start` 1, `stop` 1) |
| component in §8.1 | telemetry-patterns.md §8.1 | 3 (`fabric`, `scenario`, `ui`; `store` is a fourth unlisted component, counted under segments) |
| every name a literal | node-identity-and-state.md §9 rule 2 | 1 site (`concat!` in `crates/rdm-admin/src/control_plane.rs:1254`) |

Counts are by name. `rdm.node_admin.build.reject.via-` is the single `concat!` site: it emits 22 names `rdm.node_admin.build.reject.via-<reason>` (each reason a literal macro argument, all kebab-case, five segments, verb `reject`), plus the literal fallback `rdm.node_admin.build.reject.via-invalid-intent`. Those 22 are not counted separately; none can be found by grepping the full name.

Known examples confirmed: `rdm.node_admin.node.connections.delete.via-stop` (six segments, `node_self.rs:1048`) and `rdm.node_admin.seat.update.via-primary-updated` (banned `seat`, `fabric_handover.rs:213` and one more site).

Component distribution of the 584 names: `node_admin` 355, `mesh` 127, `node_rpc` 55, `node` 19, `testkit` 18, and one each `scenario`, `ui`, `fabric`, `store`.

## Reading the findings

- The six-segment, `get` and `*-ed` verb names (`rdm.node_admin.node.*`, `fabric.*`, `mesh.*`, `build.*`) are the Node RPC call spans and reply-frame spans in `crates/rdm-core/src/names.rs` (lines 75-89, 115-116, 149-152, 182, 228-235) and their writers; `names.rs` documents them as `rdm.node_admin.<entity>.<verb>.via-call`. No page names them: the pages describe the ops (`node.connections.delete`), not the span names, so there is no page line to rename with.
- No banned-word name is on a page, so each banned-word rename is a code-only rename. 
- `rdm.fabric.issuer.update.via-renewal` is named in `docs/architecture/lifecycles/mesh-issuer-renew.md:21`; its sibling `rdm.node_admin.issuer.update.via-renewal` (`node-issuer-update.md:22`) conforms. A rename (component `node_admin`, or a §8.1 amendment adding `fabric`) is page and code together, and a §8.1 amendment is Luke's.
- `docs/architecture/lifecycles/fabric-ca-renew.md:43` names `rdm.fabric.root.update.via-caller`; the code has no such span (`rg 'fabric.root.update'` over `crates/` finds none), a page-names-it, code-lacks-it difference reported, not bent.

## Findings

| span name | file:line | rule broken | page line naming it |
|---|---|---|---|
| `rdm.fabric.issuer.update.via-renewal` | `crates/rdm-admin/src/cert_renewal.rs:403` | component not in §8.1 (`fabric`) | docs/architecture/lifecycles/mesh-issuer-renew.md:21 |
| `rdm.mesh.connection.reject.via-fenced-incarnation` | `crates/rdm-transport/src/topology.rs:181` | banned word (`fenced`) | none |
| `rdm.mesh.connection.update.via-restart-address-retired` | `crates/rdm-transport/src/topology.rs:1658` | banned word (`retired`) | none |
| `rdm.mesh.entry.resolve.via-fabric-seat-moved` | `crates/rdm-admin/src/rafka_time.rs:73` | banned word (`seat`) | none |
| `rdm.mesh.seat.update.via-announcement` | `crates/rdm-transport/src/topology.rs:2478` | banned word (`seat`) | none |
| `rdm.mesh.topology.reject.via-departed-birth` | `crates/rdm-transport/src/topology.rs:4043`; `crates/rdm-transport/src/topology.rs:4059` | banned word (`departed`) | none |
| `rdm.node_admin.build.get.via-call` | `crates/rdm-core/src/names.rs:182` | verb not on whitelist (`get`) | none |
| `rdm.node_admin.build.reject.via-` | `crates/rdm-admin/src/control_plane.rs:1254` | name not a literal: `concat!` stem, reasons are macro arguments | docs/architecture/node-rpc-envelope.md:1232-1234 (the sibling reasons `via-unservable-fact`, `via-fetch-incomplete`, `via-fetch-failed`) |
| `rdm.node_admin.build.reject.via-not-authority` | `crates/rdm-admin/src/control_plane.rs:175` | banned word (`authority`) | none |
| `rdm.node_admin.build.reject.via-seat-yielded` | `crates/rdm-admin/src/build_drive.rs:972` | banned word (`seat`) | none |
| `rdm.node_admin.build.update.via-departed-claimant` | `crates/rdm-admin/src/control_plane.rs:325`; `crates/rdm-admin/src/control_plane.rs:370`; `crates/rdm-admin/src/control_plane.rs:1037` | banned word (`departed`) | none |
| `rdm.node_admin.build.update.via-seat-moved` | `crates/rdm-admin/src/build_op.rs:613`; `crates/rdm-admin/src/build_op.rs:645` | banned word (`seat`) | none |
| `rdm.node_admin.deployment.delete.via-fence` | `crates/rdm-admin/src/seat_revision.rs:38`; `crates/rdm-admin/src/seat_revision.rs:46`; `crates/rdm-admin/src/seat_revision.rs:54`; `crates/rdm-admin/src/seat_revision.rs:59` | banned word (`fence`) | none |
| `rdm.node_admin.fabric.get.via-atlas-call` | `crates/rdm-core/src/names.rs:152` | verb not on whitelist (`get`) | none |
| `rdm.node_admin.fabric.get.via-atlas-row` | `crates/rdm-admin/src/fabric_atlas.rs:594` | verb not on whitelist (`get`) | none |
| `rdm.node_admin.fabric.get.via-call` | `crates/rdm-core/src/names.rs:149` | verb not on whitelist (`get`) | none |
| `rdm.node_admin.fabric.get.via-meshes-call` | `crates/rdm-core/src/names.rs:151` | verb not on whitelist (`get`) | none |
| `rdm.node_admin.fabric.get.via-nodes-call` | `crates/rdm-core/src/names.rs:150` | verb not on whitelist (`get`) | none |
| `rdm.node_admin.mesh.get.via-call` | `crates/rdm-core/src/names.rs:115` | verb not on whitelist (`get`) | none |
| `rdm.node_admin.mesh.get.via-nodes-call` | `crates/rdm-core/src/names.rs:116` | verb not on whitelist (`get`) | none |
| `rdm.node_admin.node.config.get.via-call` | `crates/rdm-core/src/names.rs:80` | 5 segments (6 found) | none |
| `rdm.node_admin.node.config.update.via-call` | `crates/rdm-core/src/names.rs:81` | 5 segments (6 found) | none |
| `rdm.node_admin.node.connections.delete.via-call` | `crates/rdm-core/src/names.rs:79` | 5 segments (6 found) | none |
| `rdm.node_admin.node.connections.delete.via-connections-deleting` | `crates/rdm-node/src/node_self.rs:698` | 5 segments (6 found) | none |
| `rdm.node_admin.node.connections.delete.via-delete-connections` | `crates/rdm-node/src/node_self.rs:708` | 5 segments (6 found) | none |
| `rdm.node_admin.node.connections.delete.via-mesh-primary` | `crates/rdm-admin/src/status_rpc.rs:1743` | 5 segments (6 found) | none |
| `rdm.node_admin.node.connections.delete.via-stop` | `crates/rdm-node/src/node_self.rs:1099` | 5 segments (6 found) | none |
| `rdm.node_admin.node.connections.deleted.via-reply-frame` | `crates/rdm-core/src/names.rs:231` | 5 segments (6 found) | none |
| `rdm.node_admin.node.connections.get.via-call` | `crates/rdm-core/src/names.rs:78` | 5 segments (6 found) | none |
| `rdm.node_admin.node.created.via-reply-frame` | `crates/rdm-core/src/names.rs:228` | verb not on whitelist (`created`) | none |
| `rdm.node_admin.node.deleted.via-reply-frame` | `crates/rdm-core/src/names.rs:233` | verb not on whitelist (`deleted`) | none |
| `rdm.node_admin.node.fabric-primary.update.via-call` | `crates/rdm-core/src/names.rs:89` | 5 segments (6 found) | none |
| `rdm.node_admin.node.get.via-call` | `crates/rdm-core/src/names.rs:75` | verb not on whitelist (`get`) | none |
| `rdm.node_admin.node.joined.via-reply-frame` | `crates/rdm-core/src/names.rs:230` | verb not on whitelist (`joined`) | none |
| `rdm.node_admin.node.mesh.left.via-reply-frame` | `crates/rdm-core/src/names.rs:235` | 5 segments (6 found) | none |
| `rdm.node_admin.node.mesh.update.via-mesh-primary` | `crates/rdm-admin/src/status_rpc.rs:859` | 5 segments (6 found) | none |
| `rdm.node_admin.node.name.update.via-call` | `crates/rdm-core/src/names.rs:76` | 5 segments (6 found) | none |
| `rdm.node_admin.node.name.update.via-pending-release` | `crates/rdm-admin/src/node_rows.rs:1411` | 5 segments (6 found) | none |
| `rdm.node_admin.node.name.update.via-writer` | `crates/rdm-admin/src/node_name_update.rs:116` | 5 segments (6 found) | none |
| `rdm.node_admin.node.primary.update.via-call` | `crates/rdm-core/src/names.rs:88` | 5 segments (6 found) | none |
| `rdm.node_admin.node.start.via-workflow` | `crates/rdm-core/src/names.rs:86` | verb not on whitelist (`start`) | none |
| `rdm.node_admin.node.started.via-reply-frame` | `crates/rdm-core/src/names.rs:229` | verb not on whitelist (`started`) | none |
| `rdm.node_admin.node.stop.via-workflow` | `crates/rdm-core/src/names.rs:85` | verb not on whitelist (`stop`) | none |
| `rdm.node_admin.node.stopped.via-reply-frame` | `crates/rdm-core/src/names.rs:232` | verb not on whitelist (`stopped`) | none |
| `rdm.node_admin.node.tags.create.via-call` | `crates/rdm-core/src/names.rs:82` | 5 segments (6 found) | none |
| `rdm.node_admin.node.tags.create.via-writer` | `crates/rdm-admin/src/node_properties.rs:74` | 5 segments (6 found) | none |
| `rdm.node_admin.node.tags.delete.via-call` | `crates/rdm-core/src/names.rs:83` | 5 segments (6 found) | none |
| `rdm.node_admin.node.tags.delete.via-writer` | `crates/rdm-admin/src/node_properties.rs:79` | 5 segments (6 found) | none |
| `rdm.node_admin.node.topology.get.via-call` | `crates/rdm-core/src/names.rs:77` | 5 segments (6 found) | none |
| `rdm.node_admin.node.update.via-exit-paths-retired` | `crates/rdm-admin/src/admin/runner.rs:943`; `crates/rdm-admin/src/admin/runner.rs:966` | banned word (`retired`) | none |
| `rdm.node_admin.node.updated.via-reply-frame` | `crates/rdm-core/src/names.rs:234` | verb not on whitelist (`updated`) | none |
| `rdm.node_admin.runtime.reject.via-not-authority-capable` | `crates/rdm-admin/src/admin/boot.rs:2260` | banned word (`authority`) | none |
| `rdm.node_admin.seat.update.via-investigation` | `crates/rdm-admin/src/seat_watch.rs:264` | banned word (`seat`) | none |
| `rdm.node_admin.seat.update.via-primary-updated` | `crates/rdm-node/src/member_handover.rs:188`; `crates/rdm-admin/src/fabric_handover.rs:213` | banned word (`seat`) | none |
| `rdm.node_admin.status.remove.via-seat-moved` | `crates/rdm-admin/src/status_declare.rs:480` | banned word (`seat`) | none |
| `rdm.node_admin.status.resolve.via-seat-taken` | `crates/rdm-admin/src/status_rpc.rs:293` | banned word (`seat`) | none |
| `rdm.node_admin.status.update.via-seat-fold` | `crates/rdm-admin/src/status_rpc.rs:360` | banned word (`seat`) | none |
| `rdm.node_rpc.status.remove.via-seat-moved` | `crates/rdm-node/src/node.rs:1348` | banned word (`seat`) | none |
| `rdm.scenario.wedge.resolve.via-detector` | `crates/rdm-test/src/wedge.rs:499` | component not in §8.1 (`scenario`) | none |
| `rdm.store.open.via-fold` | `crates/rdm-node/src/store_log.rs:523` | 5 segments (4 found) | none |
| `rdm.testkit.fault.update.via-partition-subset` | `crates/rdm-test/src/link_faults.rs:125` | banned word (`partition`) | none |
| `rdm.ui.build.create.via-node-admin` | `demo/admin-ui/src/control.rs:123` | component not in §8.1 (`ui`) | none |