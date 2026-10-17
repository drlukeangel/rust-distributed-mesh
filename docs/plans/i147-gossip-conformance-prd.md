# i147 — gossip conformance: no stale frame undoes a newer fact, no required fact is lost to arrival order

**Status:** approved 2026-10-16 (Luke, Q17 of the i144/i146 stream, arm A on the a1 architect's shape).
Nothing built; the first landing is one proven regression. **Tracking issue:** [drlukeangel/rafka-v2#3130](https://github.com/drlukeangel/rafka-v2/issues/3130). **Record:** this file; every state or
remaining-work update goes in an epic's row below. **Owner:** the i143 lead; an RDM engineer in a pool
lane. **Separate from i146:** gossip stays best-effort (gossip.md §7, R-NS2); this unit fixes what
receivers do with the frames they receive. No global counter, no acknowledgements, no reliable-gossip
layer.

## Context

The receive path has one check (`crates/rdm-transport/src/receive_order.rs`), but only 9 of the 22
frame kinds carry an order key it can apply, each with its own field and its own idea of "source"
(Digest; Members and MembersDelta; NodeDeleting, NodeRestarting, NodeStopped; Seated; PrimaryUpdating
and PrimaryUpdated). The other 13 (NodeDeleted; MeshStatus, FabricStatus; Concern;
NodeConnectionsDeleting, NodeConnectionsDeleted; NodeLeaving, NodeLeft; NewFabricPrimary;
StateCommitting, StateCommitted, TrafficOpening, TrafficOpened) pass the check untouched and are
taken as they arrive, each handler applying whatever data rule it already has (receive_order.rs:25-26,
cell :471-472). For a keyed frame, an equal key is accepted and the handler absorbs the copy. So gossip
has five different staleness shapes across nine kinds and nothing at all for thirteen, and no
duplicate rule anywhere.

Two questions were being asked by one check. **Duplicate**: the same frame delivered again by the
fan-out; answered by the frame's own identity (the exact operation and birth, the chunk's key), not
by a transport counter. **Stale**: a frame about a subject older than what the receiver holds about
that subject; answered by the subject's own revision, which differs by category. A per-author
sequence was considered and refused (Luke, a1 2026-10-16): it would drop a terminal fact because an
unrelated newer frame arrived first.

## The rule

No stale frame may undo a newer fact, and no required fact may be lost merely because another frame
arrived first. Every active frame kind has an explicit stale/duplicate rule, stated on gossip.md and
covered by a cell.

## The frame-contract categories

| Category | Required behaviour |
|---|---|
| Replacement state | Install only a newer revision of the same source projection |
| Snapshot or delta | Install atomically against the correct source revision and base; equal-key chunks remain valid |
| Operation fact | Match the exact operation and birth; a duplicate does nothing; an unrelated message cannot supersede it |
| Reserved frame | Refuse by name and apply nothing |

The table over all 22 kinds landed on gossip.md §3.1 "Receive rules: stale, duplicate and gap, per frame"
(Luke's yes 2026-10-16, with the a1 architect's seven corrections). e1's deliverable is the first proven
regression against it; e2's are the cells for every row.

## Requirements

| # | Requirement | State | Evidence today | Epic |
|---|---|---|---|---|
| R1 | the frame-contract table: every frame kind maps to its receive category, subject, ordering and identity | HOLDS | gossip.md §3.1 approved table; `every_frame_kind_has_a_row_in_the_receive_rules` covers all 22 Frame variants; e1 gate GREEN | e1 |
| R2 | replacement-state frames install only a newer revision of the same source projection; an equal revision is a duplicate and does nothing | GREEN cells; batch landing pending | Digest and seat classifiers use actual installed projections under their installation locks; 63 live conformance cells and 9 receive/seat units GREEN, batch gate pending | e2 |
| R3 | snapshot and delta frames install atomically against the correct source revision and base; reordered chunks still complete; equal-key chunks valid | BUILD | §3.3 states the top-up; the atomic rule is unproven | e2 |
| R4 | operation facts match the exact operation and birth; duplicates do nothing; an unrelated newer frame never supersedes a terminal fact | BUILD | 13 kinds taken as they arrive | e2 |
| R5 | reserved frames (NodeLeaving, NewFabricPrimary, and every other reserved position) are refused by name and mutate nothing | BUILD (check) | `rdm.mesh.membership.reject.via-node-leaving` exists for one | e2 |
| R6 | a publisher change: a delayed predecessor's frame cannot overwrite the successor's projection | BUILD | the seat-revision rule covers Seated only | e2 |
| R7 | recovery through a topology read followed by stale gossip: no rollback | BUILD | — | e2 |
| R8 | duplicated or contradictory checks removed from the receive path; a field added only where a demonstrated gap cannot be closed with the information already carried, each as its own packet with reseal and marker | BUILD | five key shapes | e2 |
| R9 | gossip stays best-effort: no global counter, no acks, no reliable layer; nothing from i146 enters it | HOLDS | §7 | every epic |
| R10 | `MeshStatus` and `FabricStatus` carry no publisher revision, so a delayed predecessor's frame is not refused by revision; closed by the envelope's `order` (the author's committed revision), e3 | BUILD (gap, recorded on gossip.md) | `(publisher: String, changed_at_rafka_ms)` only | e3 |
| R11 | `Concern` is retired (Luke Q23, 2026-10-16): position 10 reserved and refused by name; `seat_watch`'s `Trigger::Concern` and the `concerns` book deleted; the seat watch runs from its own silence evidence; the cert epic's "raises `Concern`" lines become "the row is surfaced on `fabric.get`" | HOLDS | publisher, ConcernHeard/ConcernInbox and watch trigger/re-feed removed; position 10 refused on both channels; silence-only recovery and seat control GREEN | e2 |
| R11a | withdrawn (Luke, 2026-10-16): no per-row `effect` rule. The limit is the envelope (e3: header and body); the receive-rules table documents what each frame is, and the one sentence before it (a receiver decides nothing on a frame) stands | — | — | — |
| R12 | a delta whose base is ahead of the installed version is a gap: repair requested (§3.3, §6), spanned `via-gap`, never logged stale; equal revision with different content is `via-inconsistent`, refused by name | BUILD | mismatched base handling unproven; no inconsistency refusal | e2 |
| R13 | round hooks drive no checklist and reopen nothing by construction; their receive effects are the span and the mesh-admin's carrier, which carries an ordinary node's check-in only while its book holds the round open (`carried = closed-round` otherwise); no round evidence is exposed to passive receivers (Luke Q21: x, 2026-10-16) | HOLDS | Q21: x; RoundReceipts dedupes identity before span/carrier; RoundBook::carry_check_in returns the named existing open-round decision; 14 live hook cells GREEN | e2 |

## § OTLP span-assertion bar

| Span | Owner | Attributes | Cells |
|---|---|---|---|
| `rdm.mesh.membership.reject.via-stale` | the receiver | frame, source, held_revision, frame_revision | the newer-then-older cells |
| `rdm.mesh.membership.update.via-duplicate` | the receiver | frame, operation or key, outcome `ignored` | the repeat cells |
| `rdm.mesh.membership.reject.via-reserved` (the existing `via-node-leaving` generalised) | the receiver | frame position | the reserved cells |
| `rdm.mesh.membership.update.via-round-frame` | the hook receiver; carrier decision stays at the mesh-admin | full hook identity, via, forwarded_by, carried (`carried`, `closed-round`, `node-admin`, or non-carrier outcome) | first/open/forwarded/closed/admin-author hook cells; repeats have no second ordinary span |
| control: a terminal fact applied after an unrelated newer frame carries its ordinary apply span | — | — | the out-of-order fact cell |

## § Per-sprint seed-data obligation

NO NEW ENTITY. No wire change unless a demonstrated gap needs a field (then `wire format moved`,
mesh-audit reseal, as its own packet). No new gossip frame and no new `MemberStatus`.

## Implementation plan

#### e1 — the table and the first proven regression

- **Goal → gap.** The frame-contract table on gossip.md; one unkeyed MeshStatus with equal revision and different content refused as inconsistent, with its RED and GREEN flames (Luke, Q19=A).
- **Deliverables.** The red cell of the regression (the table is on the page); its fix in the receive
  path by the frame's own identity; the flame before and after; the gossip.md section as exact old →
  new to Luke; a gossip change, so Luke's permission is Q17 and the flame is required.
- **Acceptance cells.** `an_unkeyed_mesh_status_with_equal_revision_and_different_content_is_refused_inconsistent`
  (the regression); `every_frame_kind_has_a_row_in_the_receive_rules` (a unit cell over the catalog against the page's table).
  RED first.
- **State.** **LANDED** through `rebase-gate-land.sh`, 2026-10-16, lane `e7s3`, branch `codex/i147-e1-gossip-conformance`, Q19=A. RED SHA `35af8ca1`; GREEN/landed SHA `1da16141`. Before trace `f6fc35d858049ff6c75782276d66a969`; after trace `7af27c38a6aa16ca6f5dbf040b0ebd1e` shows `rdm.mesh.membership.reject.via-inconsistent` with frame `mesh-status`, subject `mesh1 mesh1.admin.1`, revision `100`, held status unchanged. Verification: live regression 1 passed, 0 failed (0.06 s); receive-order unit stem 8 passed, 0 failed (including the catalog); fast landing gate 1,623 passed, 0 failed, 0 ignored across 62 libtest summaries, 261 s total. Discovery: 1,774 tests against 1,772 on main; release-only jobs excluded by the fast gate. Gate log: `/home/admin/rust-distributed-mesh-deploy/gates-e7s3-1da16141.log`. The receive framework classifies against the held status under its installation lock, replacing the MeshStatus handler check. Span reference regenerated. The approved architecture table was already on main at `ab129725`; code now meets its named inconsistency refusal for MeshStatus. No architecture or wire change. No unanswered packet; R10 and e2 remain outside this landing.

#### e2 — every row, one defect at a time

- **Goal → gap.** Each remaining row of the table whose rule the code does not meet is a defect with
  its own red → green cell; rows the code already meets get their cell and no change.
- **Acceptance cells (the a1 set, each against its defect).** newer state then older: no regression;
  a terminal fact after an unrelated newer frame: applied; a fact repeated: no duplicate effect;
  snapshot chunks reordered: complete and atomic; publishers change: the predecessor's delayed frame
  does not overwrite; recovery through a topology read, then stale gossip: no rollback; each reserved
  frame: named refusal, no mutation.
- **Order.** MeshStatus and FabricStatus; NodeDeleted; StateCommitting, StateCommitted, TrafficOpening, TrafficOpened (one grouped landing, Luke 2026-10-16); NodeDeleting, NodeRestarting, NodeStopped (one grouped row); Concern retirement (Q23); R11a withdrawn; replacement state; snapshot or delta; reserved positions; recovery through a topology read. Luke’s reporting rule: outside-i147 gate reds are recorded in one line without editing their cells; continue the next row on the same branch and land the batch when the gate is green. Report a successful landing, a needed ruling, or a blocker spanning more than one row. R10 stays open throughout e2; e3 starts only after the last e2 row.
- **State.** MeshStatus, FabricStatus, NodeDeleted (`d4662736`), the grouped round hooks (`bad08475`) and NodeDeleting / NodeRestarting / NodeStopped (`f261a198`) accepted. The lifecycle row's new landing gate passed: 1,664 passed, 0 failed, 62 summaries, 76 s; Luke accepted both disputed earlier reds as outside-i147 gate-load sensitivity after the isolated 2×3 matrices. Concern Q23 LANDED at `00ee6f69` through rebase-gate-land.sh (197 s, 1691 passed, 0 failed, 0 ignored across 62 summaries): publisher, inbox, watch trigger and re-feed deleted; position 10 refused; own-silence recovery and unchanged-seat control proven. Lane `e7s3` rebased onto `34792687`, branch `codex/i147-e2-concern-reserved`. R11a is withdrawn; no effect column. Digest replacement candidate `03613e10` is GREEN in its cells but NOT LANDED: the land-script gate failed in `node_stop.rs:147`; outside-i147 read retained, no cell edit or rerun. Per Luke’s new rule, Seated and PrimaryUpdating/PrimaryUpdated are now GREEN (63 live conformance cells, 9 receive/seat units) on the same branch for the next batch landing. R10 remains open; e3 remains not started. A gate red outside i147 is read and reported to its owner, never fixed here.

| Frame row | State | Evidence |
|---|---|---|
| `MeshStatus` | GREEN | Lane `e7s3`, branch `codex/i147-e2-mesh-status`. RED SHA `09d42858` (1 passed, 2 failed); GREEN code SHA `2697fa33` (3 live cells passed, 0 failed, 0.06 s). Stale before `ecc86037d4bf8ea46dff8faeb0c9a95c`, after `cea209eb2b76c8e7512a2161c0a12859`; duplicate before `7f4660d772157e251095c8c1cbfdcb79`, after `e7c2c444cc889b9d71011b5fb287a03e`. Stale names frame, subject, held key and frame key; duplicate names frame, declared identity and outcome `ignored`. Both assert the held projection and its change-notification count do not move. Forwarding metadata changes the wire bytes to exercise RDM dedup without changing authored identity/content. Classification stays in the receive framework under the status installation lock; the handler's generic debug substitute is removed, module documentation updated and span reference regenerated. Pre-landing fast gate at `2697fa33`: 1,633 passed, 0 failed, 0 ignored across 62 recorded libtest summaries, 84 s; discovery 1,776 against main's 1,774. Gate log `/home/admin/rust-distributed-mesh-deploy/gates-e7s3-2697fa33.log`. Delivery uses `rebase-gate-land.sh`, including its gate. Candidate `c393b290` gate RED (node_restart, provider_state), no landing: RED restart flame `8aa0dd8f1269ee29e7469d44ea485ae1`; RED recovery flame `4a08384b10a43575caf30bb032c4fa98` (central Jaeger `http://192.168.68.99:16687`). Recovery teardown aborts tasks at 21:47:48.664 after WaitForNodeReady starts at 21:47:48.631 and before the final receipt. The restart release timestamp moves before Continue; recovery teardown waits for the terminal receipt within the original deadline. Corrected stems: 6 passed, 0 failed (41.56 s), resumed-restart GREEN flame `099bb149d21bb334d598222bf24dd3fd`, settled-recovery GREEN flame `72517d6fa18433f79fa1f15b8e05cb7b` (local Jaeger `http://localhost:16686`). No architecture or wire change; R10 remains open; no ruling packet needed. Envelope notes: subject `(mesh, publisher)`; ordering `changed_at_rafka_ms`; identity `(mesh, publisher, changed_at_rafka_ms)`; status content distinguishes duplicate from inconsistency. Publisher is currently a string without a committed revision or exact author birth; forwarding metadata is excluded. |
| `FabricStatus` | GREEN | Lane `e7s3`, branch `codex/i147-e2-fabric-status`. RED SHA `870fe399`: 0 passed/3 failed (0.06 s). GREEN code SHA `3f830db0`: all 6 live receive-rule cells passed (0.06 s), including all 3 MeshStatus cells. Stale before `6ca2949ec512e5facb98aef7d93a160d`, after `08472821f369e1186b01c69c8cab8f1c`; duplicate before `da985d5d2b5a4a5b1309303c0348cb1e`, after `8ed68b1a8c4977c9aebed8ec566d9446`; inconsistent before `a88a5dea06fee1c1aa02c6a3102d0c12`, after `89fd0fc7555d8af478f5e7d502c87422` (local Jaeger). Each cell proves receipt and no apply effect, preserving held status/publisher/timestamp. Named outcomes carry frame/subject/both keys, frame/identity/ignored, or frame/subject/revision respectively. Both status kinds now use the receive framework’s shared classifier under the held projection lock; the FabricStatus handler check is removed. Receipt instrumentation supplies the live before/after flame; module doc and generated span reference updated. Delivery uses `rebase-gate-land.sh`, which enforces its fast gate. No architecture or wire change; R10 stays open. No other initiative’s cells are modified. Envelope notes: subject `(fabric, publisher)`; ordering `changed_at_rafka_ms`; identity `(fabric, publisher, changed_at_rafka_ms)`; status content distinguishes duplicate from inconsistency. Publisher is currently a string without a committed revision or exact author birth; forwarding metadata is excluded. |
| `NodeDeleted` | GREEN | Lane `e7s3`, branch `codex/i147-e2-node-deleted`. RED SHA `f0a4a173`: terminal-after-unrelated-traffic passed, repeat failed only for missing via-duplicate (1 passed/1 failed, 0.06 s). GREEN code SHA `5200cc79`: 8 operation/status cells passed, 0 failed (0.07 s). Before repeat `83766273424177d1cc6837ecef7c7b9f`, after `d6b785406239146d16897dc05ffcc715`; before control `99d3af602032128f4b1a489a948160f9`, after `40465e0624bbfb76f27856467ac2b936` (local Jaeger). Control applies a status at timestamp 10,000, then NodeDeleted carrying event time 1: its ordinary removal span says was_held=true. Repeat emits via-duplicate and changes neither the retained fact nor birth notifications; only one removal occurs. The receive framework classifies against the retained deletion under its installation lock; the handler’s operation-identity dedupe is removed. No separate receipt history or counter; ordinary local deleted-retention stays bounded and a repeat does not refresh it. Module doc and generated span reference updated; delivery is through rebase-gate-land.sh and its fast gate. Envelope notes: subject `(node_id, incarnation)`; identity `(kind, build_id, attempt, operation, node_id, incarnation)`; no order against unrelated traffic; `event_at_rafka_ms` is evidence only; `name` supplies the subject path. The frame has `op` and `forwarded_by`, but no declared author birth: neither forwarder nor transport sender supplies it. No wire/architecture change, no R10 closure and no other initiative’s cell edit. |
| `StateCommitting`, `StateCommitted`, `TrafficOpening`, `TrafficOpened` | GREEN (Q21: x) | Lane `e7s3`, branch `codex/i147-e2-round-hooks`, rebased onto main `9295f151` before implementation. Original repeat RED `3dc216a5` (rebased `e5da6b09`); Q21 carrier-cell RED `dc878ae3`: 2 passed, 14 failed, 0 ignored (0.09 s). The 4 status-as-round-completion probes are withdrawn. Framework admission matches the full hook identity before either its ordinary span or carrier invocation. Only the admin’s existing open-round book decides carrying; its check is factored into RoundBook::carry_check_in and returns carried, closed-round or node-admin to the ordinary receive span. Forwarded metadata supplies no authority and cannot prevent an ordinary open-round check-in from being carried once. No checklist or lifecycle update comes from receipt. RED forwarded StateCommitted traces `c942cbb101284adf0351a99a9ed78223`, `f470712ef975576e1cd671ba32d9f580`; closed trace `3bc14b25d47b0a1b972995883801efce`. RED forwarded TrafficOpened traces `3f7385448ea1044c536008d44f787f1d`, `78dde0daa185c4cc6154da094856c995`; closed trace `ab3151916d82bbd7e07a30877415c2e7` (local Jaeger). GREEN code SHA `24fe82d4`: 22 live cells passed, 0 failed, 0 ignored (0.10 s), including 14 hook cells, both NodeDeleted cells and all 6 status cells; 8 receive-order unit cells passed, 0 failed (0.00 s). state-committing: before repeat `212e96f68cc304bcc637c6d11ad46fa3`, after duplicate `afa02230435278ed33c0325dc38cb7e4`; state-committed: before repeat `3f5968ab7d8147c6bbae4fc67e326efd`, after duplicate `d79da62c43e4339fbbc0e100efaccb3f`; traffic-opening: before repeat `53c6b3f3e0724c38165eee57cb1e0bcb`, after duplicate `25bf25beb88814f02e47a9ca2b953146`; traffic-opened: before repeat `417bf129e0aba3217d4129cacd4ad2c0`, after duplicate `0310c1fa3f37927a55105574139defa7`. Forwarded StateCommitted after carry `fa1f58b774ede0758d8ee308c2654db9`, after repeat `f07b8086d072be9318d3e6d14af735d7`; TrafficOpened closed after `2d973a6e1a82a578ec6cfb34696438c8`. All named flames retrieved from local Jaeger. Live backbone receipts, carrier invocations, birth notifications and the directed checklist’s completion/tick are asserted: gossip never checks in a birth or reopens a round. The first 22-cell run took 1.10 s, including live mesh/backbone handshake and teardown barriers; final run 0.10 s with stronger checklist assertions, unchanged 5 s bounds. Module documentation kept true, generated span reference refreshed. Delivery through rebase-gate-land.sh with its fast gate. No outside-i147 cell edits; no unanswered packet. [Q21 packet ruled x](rulings/i147-q21-round-evidence.md), Luke 2026-10-16. Envelope notes for e3: author; subject is the exact birth `(node_id, incarnation)`; order = `Operation(identity)`, with identity `(kind, fabric_id, build_id, attempt, operation, node_id, incarnation)`; no ordering field. Event timestamps and forwarded_by do not order or identify the fact. Current publisher is a path string without exact author birth. No wire, snapshot or architecture change. R10 stays open; e3 not started. |
| `NodeDeleting`, `NodeRestarting`, `NodeStopped` | LANDED `f261a198`; accepted | Lane `e7s3`, branch `codex/i147-e2-lifecycle-facts`. RED `38927162`: 3 passed, 9 failed (0.08 s). Repeats overwrite retained event-time evidence; a version-refused identity can reopen at a higher carried version. Held completion already prevents reopening but lacks the repeat outcome. GREEN code `7daae9dc`: 34 live lifecycle/operation/status cells passed, 0 failed (0.12 s), including all 12 new lifecycle cells; 9 receive-order/lifecycle units passed, 0 failed (0.00 s). node-deleting: repeat before `72e0a34444ca26e25fbe82ac3f74f8e0`, after `2dd2391662f1f5e63f4233d5a30ace13`; completion before `bf09d6eab320cfa6f4fd714d04fcbad2`, after `b3c9c0810559dfe4da0f553b395f080d`; carried-version refusal before `e4a3aebb878f89eb16616c12d3b72c2b`, after `f92cd8390a7f3f3255091478fb72e0c5`; apply/routing control `c921bfea4e5e3ef1e1839dee4d40a27c`; node-restarting: repeat before `98b830f68e56372c811be313ead5d6cc`, after `a2ce8dc4b7576e18552e035fe3fafce5`; completion before `9ea1857b8e78c42bb338cae1ef4ad3cf`, after `503b1f8bcdb725a6d7a168830ee59a2f`; carried-version refusal before `7e4ff3fb9ddf5d470c2b91a27acad331`, after `f8138dc467da6c35c85264687d4987b3`; apply/routing control `b91804ceb124747593b71e678cc5e123`; node-stopped: repeat before `3e34adb89ec19bef8953d3f219a4c063`, after `53bd90c6c0a97c293d02fbc43da16ce6`; completion before `7c8e9c193faab22f6bf085a5c269e50a`, after `550a99107776e0e835b6fb7f0c255f98`; carried-version refusal before `dcda23ff52b62d0c96a71af6d5290ee7`, after `7df077be858b6ef59a292a9eb54ef267`; apply/routing control `23ee2b3c12e3d57ad23f5edd3a894779`. All RED and GREEN lifecycle receipt flames retrieved from local Jaeger. The live cells assert retained operation/digest content, birth and overlay notifications, routing exclusion/restoration, and named duplicate/stale spans. The stale cell records the identity at refused v10, ignores its repeat at v21, and refuses a distinct fact at v20: the carried projection floor advances independently of operation replay. Completed cells use the receiver's actual local evidence: deletion attempt terminal facts, restart node.started, and stop followed by node.joined plus node.started of the same birth. Framework admission records the full identity before judging the existing per-publisher/mesh version; the version is judged even on a duplicate, while no duplicate handler runs. Shared overlay classification replaces the book methods' own dedupe and completion checks under their installation lock; retained completion now includes exact birth. NodeDeleting still changes only the overlay read by application routing, never the held digest or RPC readiness. The existing lifecycle receive unit reused a version-refused identity for its second-publisher control (8 passed/1 failed); it now asserts that repeat is refused, then uses a fresh attempt for the independent per-publisher assertion, retaining the original sender-independence coverage. No outside-i147 cell changed. Module doc kept true; span reference regenerated. Delivery through rebase-gate-land.sh and its fast gate. Envelope notes: author is the declared PublisherId (path plus incarnation, without author NodeId); subject is the exact `(node_id, incarnation)` birth; fact order = `Operation(identity)` with identity `(build_id, attempt, operation, node_id, incarnation)`; the existing carried publisher/topology_version is separately `Revision(topology_version)` for `(publisher, subject mesh)`. These coexist as an outer Operation order plus the existing body's projection revision, with two independent receive decisions: never flatten revision into an ordering of operations, and never lose the projection check when deduping a fact. e3's one order field must preserve this composition; its exact typed shape is frozen in e3. Event timestamp and forwarded_by supply neither order nor identity. No wire, snapshot or architecture change; R10 stays open; no new question or packet; stop before Concern. Land-script candidate `be5102b5` gate RED: 1,653 passed, 3 failed, 0 ignored across 62 summaries, 231 s. No landing; no re-roll; the red candidate is recorded by the script. The transport span assertion is now fixed at `c8856575` and verified GREEN. The 2×3 comparison for each disputed cell is GREEN on both revisions; Luke subsequently accepted both as outside-i147 gate-load sensitivity and authorized the new `f261a198` landing, whose gate passed: 1,664 passed, 0 failed, 62 summaries, 76 s; both watched cells passed, neither changed. Full comparison and precise gate observations immediately below. |

| `Concern` (Q23, R11) | LANDED `00ee6f69`; gate GREEN | Lane `e7s3`, branch `codex/i147-e2-concern-reserved`, initial base `34792687`; land script rebased onto `0816c629`. GREEN code `169d63fa`, rebased/landed `00ee6f69`; RED `e3fcfe89`, rebased `8d3242ce`: 0 passed, 2 failed (64.10 s): missing named position refusal; recovery completed but published Concern. GREEN: three targeted checks passed (72.45 s), 11 seat-watch units passed (0.00 s); final reserved/recovery checks with exact-birth matching passed 2/2 (77.95 s). Position 10 on mesh and backbone emits `rdm.mesh.membership.reject.via-reserved` with frame, position and via; held digest, mesh/fabric status, seats, departure state, overlays and notifications remain unchanged. The lost-fabric-primary cell proves own-silence investigation (`trigger = silence`, exact original node_id/incarnation, `exit_proof_row = put`) before replacement; no Concern publication or Concern-driven re-feed. The existing cut-off-running-holder cell retains its investigation, seat and iroh-shape assertions; obsolete Concern expectations are replaced, no bound widened. Publisher, ConcernHeard/ConcernInbox, watch trigger and re-feed deleted; the existing own-silence scan remains. Three obsolete publication/receipt unit contracts are replaced by own-view, ready/self-birth exclusion and exact-birth unit contracts; no discovery drop. The module doc and generated span/frame references are current. Cert-plan stalled-renewal lines now surface the row on fabric.get (Q23). RED publication [17c51f96e0bf103571b31a8f39d30683](http://localhost:16686/trace/17c51f96e0bf103571b31a8f39d30683); final silence investigation [65a7b9e034e04eaf0a9d82b3d59bb16e](http://localhost:16686/trace/65a7b9e034e04eaf0a9d82b3d59bb16e); replacement [aac919a58ebe789ee54db97c538f8bf1](http://localhost:16686/trace/aac919a58ebe789ee54db97c538f8bf1); reserved mesh [326a4b97a44142b9886c578bdd97ca4a](http://localhost:16686/trace/326a4b97a44142b9886c578bdd97ca4a), backbone [f0ecb04308ec325910362b25dbcc9d0f](http://localhost:16686/trace/f0ecb04308ec325910362b25dbcc9d0f). All selected traces retrieved; full spans and outputs preserved under `target/evidence/concern-reserved/{red,green,final}/`. Effect: refuse position 10, install nothing and carry nothing. Envelope notes: reserved; no author/subject/order fields are needed to act on this position, whose existing body is decoded only to refuse it. No wire-layout or architecture change. R11a was subsequently withdrawn by Luke; no effect column or catalog change is needed. R10 remains open; no new question. Land-script gate GREEN: 1691 passed, 0 failed, 0 ignored across 62 summaries, 197 s. Provider_state (3 passed), sticky_seat (5 passed) and fabric_primary (4 passed) all GREEN. Gate log `/home/admin/rust-distributed-mesh-deploy/gates-e7s3-00ee6f69.log`. Stopped after Concern; replacement state not started. |

| `Digest` | GREEN cells; batch landing pending | Lane `e7s3`, branch `codex/i147-e2-digest`, rebased onto `bb3569c8`. RED `9f0ffd9a`: 2 passed, 4 failed (0.05 s). Newer-sequence and new-incarnation controls pass; stale and below-read-installed sequence have no required outcome; a rejected equal-sequence copy still registers addresses. A duplicate refills a forgotten address and inconsistent content installs an unrelated endpoint. RED receipt flames show `applied = false, addresses_registered = 1`. A further read-floor probe proves the separate receive history can name refused 90 as the floor after 100 was installed; the final cell sends 90 then 80 after a real topology-read installation, and both refusals name 100. GREEN: 41 live conformance cells passed (0.13 s); final six Digest cells plus the original digest-sequence cell passed 7/7 (0.05 s), 8 receive-order units passed (0.00 s), exact publisher-handover cell passed (0.19 s). The existing publisher-handover assertion now requires Digest's via-stale and retains channel, subject and both keys. No other initiative's cell or bound changed. Classification is against the actual installed heartbeat, under the installation lock, using WireDigest content (membership status is absent); the handler's own <= check and the separate receive-history floor are removed. A rejected digest returns no address and supplies no clock observation. Runtime and lineage data checks remain. The ordinary via-digest span proves a higher sequence and a new incarnation apply; refusal/duplicate cells preserve held projection/status and digest/birth/overlay notifications. Stale before [cfa131f6b322421521ff5edf1fcad746](http://localhost:16686/trace/cfa131f6b322421521ff5edf1fcad746), after [71d49d2c583585eb6167983ef805e894](http://localhost:16686/trace/71d49d2c583585eb6167983ef805e894); duplicate address receipt before [c4cfd6dd442dfbc869fd8abbc775ce57](http://localhost:16686/trace/c4cfd6dd442dfbc869fd8abbc775ce57), after [4cbe8dcd2c6a9f0c83e58baf91ac324a](http://localhost:16686/trace/4cbe8dcd2c6a9f0c83e58baf91ac324a), named duplicate [a3ead98d65d64d3d4f458a587ea03728](http://localhost:16686/trace/a3ead98d65d64d3d4f458a587ea03728); inconsistent address receipt before [f77964f237af10c7e0e9eb0281325e3d](http://localhost:16686/trace/f77964f237af10c7e0e9eb0281325e3d), after [a0739f94d1089cae8124dc607c1b2db4](http://localhost:16686/trace/a0739f94d1089cae8124dc607c1b2db4), named inconsistency [c281d37a9c187b161b3b7f383ca8af41](http://localhost:16686/trace/c281d37a9c187b161b3b7f383ca8af41); actual read floor [e8ab71029982c8f0fca2e7a17941e83a](http://localhost:16686/trace/e8ab71029982c8f0fca2e7a17941e83a). GREEN refused receipts all show `applied = false, addresses_registered = 0`; all named traces retrieved. Full before/after spans, outputs and retrieved trace JSON are preserved under `target/evidence/digest-replacement/{red,green}/`. Module doc and generated span reference kept true. Envelope notes for e3: source is the author's exact birth URI `rdm://fabric_id/node_id/incarnation`, never the transport forwarder; subject is the exact birth; sequence is digest_seq. A new incarnation starts its own sequence; time supplies no order. The primary's held membership status is absent from heartbeat data and excluded from content equality. No wire field, architecture change or new packet is needed. R10 remains open; e3 not started. GREEN code `03613e10`. Land-script gate RED: 1696 passed, 1 failed, 0 ignored across 62 summaries, 257 s. Only red: node_stop::a_stop_parks_the_process_and_a_start_rejoins_it_as_the_same_birth at crates/rdm-test/tests/node/node_stop.rs:147. The stop/start workflow completed; the assertion expects reused=false but reads true. Provider_state and sticky_seat passed. No other initiative cell edited, no rerun or fix; one-line owner read in this row. Seated and PrimaryUpdating/PrimaryUpdated now build on this branch for a batch landing. Outside-i147 gate read (`03613e10`): `node_stop::a_stop_parks_the_process_and_a_start_rejoins_it_as_the_same_birth`, `crates/rdm-test/tests/node/node_stop.rs:147`: start asserted `reused=false`, observed true after a hard cut; stop/start and same-birth assertions passed, and a fresh dial occurred 207 ms after the cut before the start lookup ([cut](http://192.168.68.99:16687/trace/161cc8a30f18e0751f9c7dc11af18477), [dial](http://192.168.68.99:16687/trace/e198c1bb71747d7ce49dfe3f9f84f750), [start](http://192.168.68.99:16687/trace/7935136760b2c3674ffcb8d742901781)); handed to i144/i146, cell untouched; full retrieved evidence `target/evidence/digest-replacement/gate-red/`. |

| `Seated` | GREEN cells; batch landing pending | RED `e473298f`: the initial 19 seat cells have 4 passed, 15 failed (0.09 s); Seated lacked the three named outcomes and read-floor outcome. First/higher revision and independent-cohort controls passed. GREEN combined conformance: 63 passed, 0 failed (0.16 s), including 22 live seat cells; receive-order and original late-Seated unit contracts: 9 passed, 0 failed (0.00 s). The framework classifies against installed records under the seat book's lock, including records installed by a real learn_seats topology-read path; no separate cohort receive-history floor. Lower revision emits via-stale with subject and both keys; identical record emits via-duplicate with identity/outcome ignored; different content at equal revision emits via-inconsistent. Only installation emits via-announcement, and cohort, mesh and fabric-seat controls prove subject scope. Local election candidate selection remains SeatHolder::supersedes; received conflicts do not invoke its equal-revision candidate choice. The handler's independent take/supersedes check is removed. RED ordinary apply [733f135dea847a44eb103ab72e505037](http://localhost:16686/trace/733f135dea847a44eb103ab72e505037); [seated-stale](http://localhost:16686/trace/5c61d4a6bba6656b019e5e7e3f1ba790), [seated-duplicate](http://localhost:16686/trace/23d141fac4205f8bc52197c4bc7aabc2), [seated-inconsistent](http://localhost:16686/trace/c1ff5093b88b29cb821d6dc807e6fa30), [seated-read-floor](http://localhost:16686/trace/39c9cfbe0414eba39a08b14ebb0ff608), [seated-newer](http://localhost:16686/trace/c5bd8b78d8cfd7dc6dccf269f764efae). Envelope notes for e3: source is the author (not necessarily the holder), currently absent as an exact birth in this frame, never inferred from the transport forwarder; subject is the complete cohort (mesh, node_type) or the fabric seat, with node-admin normalized to mesh-primary; sequence is the holder revision, and the cohort's installed floor survives a change of author. No wire field or architecture change. |
| `PrimaryUpdating` / `PrimaryUpdated` | GREEN cells; batch landing pending | RED `e473298f`: repeats emit second ordinary apply spans; equal conflicting update content lacks via-inconsistent; stale and read-installed floors lack via-stale. An additional live red proves an actual locally installed committed operation was misclassified via-duplicate when gossip changed the operation at the same revision ([7084ac9a986b9f3ae0f5b3e40bbc045b](http://localhost:16686/trace/7084ac9a986b9f3ae0f5b3e40bbc045b)). GREEN: the 63-cell conformance run and 9 transport units above. Receipt checks the complete row before moving any installed record or receive evidence. At one revision settled is later than updating; updating installs no holder. Any stale cohort refuses all named cohorts, and a following single-cohort control proves a refused row moved no other floor. Identical content emits only via-duplicate; conflicting holder or complete operation/body at equal revision emits via-inconsistent. The actual local take_together installation retains its committed publication content for that same comparison. Only admitted frames emit via-primary-updating/via-primary-updated; no handler dedupe or supersedes check remains in this receive path. RED repeated ordinary updating [92f09cc680362ef005220ca9e2589741](http://localhost:16686/trace/92f09cc680362ef005220ca9e2589741), updated [b7086411b56564e4438e6bce1841ee8c](http://localhost:16686/trace/b7086411b56564e4438e6bce1841ee8c); [primary-updating-duplicate](http://localhost:16686/trace/799f80cf852463f79a533909acd9dac5), [primary-updated-duplicate](http://localhost:16686/trace/8c812e54ca78d182ecb2e65df824b440), [primary-updating-inconsistent](http://localhost:16686/trace/5e299733e4e00fec273d44e7097a87d0), [primary-updated-inconsistent](http://localhost:16686/trace/ae557af9a325510c84bf768f50b6cb9f), [primary-updating-stages](http://localhost:16686/trace/76de5e2072525fe151231feff362f2a5), [primary-updated-atomic](http://localhost:16686/trace/f53a7f934109e1c0dd894e2ab24010b7), [primary-updated-installed-operation](http://localhost:16686/trace/605eda8983169f1e37f16b079c0091de), [primary-updated-newer](http://localhost:16686/trace/6a3eb76bb3b54ac59c10fd5779444afc). All selected RED/GREEN traces retrieved; full spans, outputs and trace JSON under target/evidence/seat-replacement/{red,green}/. Module doc and generated span reference kept true. Envelope notes for e3: source is the author's exact birth, currently not declared independently of the former holder/operation; subject is every named complete cohort or fabric seat; sequence needs each cohort's committed revision plus stage (updated later than updating), checked atomically as a vector for multi-seat frames. The one scalar Sequence mapping must preserve that vector and stage, not invent a frame-wide counter; full operation and all seat records are content for equal-revision comparison. A publication by another writer does not reset the cohort floor. No wire field, architecture change or new packet in e2; R10 remains open. |





**Lifecycle row verification and attribution — landed `f261a198` (Luke's requested comparison, 2026-10-16 Eastern).**

Candidate `be5102b5` had a RED fast gate: 1,653 passed, 3 failed, 0 ignored across 62 summaries,
231 s. The three failing cells were the transport span assertion, provider state and sticky seat.
At the comparison checkpoint attribution was unestablished. Luke subsequently accepted both as outside-i147 gate-load sensitivity; the new landing passed at `f261a198`.

**Transport assertion — fixed.** `c8856575` updates
`crates/rdm-transport/tests/publisher_handover.rs` to require `via-stale` with `subject`, `held_key`
and `frame_key` for NodeStopped. The other three keyed families retain their existing named
expectations. The assertion includes the span's name, so an old-name NodeStopped refusal cannot
satisfy it. The exact integration cell passed through cells.sh: 1 passed, 0 failed, 0 ignored,
0.19 s. No receive-path code changed in this commit.

**Method.** Lane `e7s3`, serial isolated exact cells, three runs of each at detached main
`bad08475`, then three of each on `codex/i147-e2-lifecycle-facts` at `c8856575`. Both cells' source
files are byte-for-byte unchanged between those revisions. The branch's receive implementation
is the gate candidate's; the additional change is the transport assertion. Every invocation used
`/home/admin/lane-queue/i143-lead/cells.sh rdm-test main -- <fully-qualified-cell> --exact --nocapture`
and `OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318`. cells.sh set the same cadence in both
states (staleness 3000 ms, gossip/backbone 500 ms); no bound changed. Each run's source SHA,
exit status, timing, full log, manifest and node evidence was preserved before the next run.

**Provider state: 2×3.** The trace links name the os-stopped clear of the original birth. Every run
observed `recorded` then `cleared/exited`, and the cell completed the recovery checks.

| Revision | Run 1 | Run 2 | Run 3 |
|---|---|---|---|
| main `bad08475` | PASS [`ad0564b99997ba56cec348f4718635f4`](http://localhost:16686/trace/ad0564b99997ba56cec348f4718635f4) | PASS [`38f4f39881a5aff69502a96d478096e3`](http://localhost:16686/trace/38f4f39881a5aff69502a96d478096e3) | PASS [`201da0f2af1b6ef9c88a7bb99e1cda39`](http://localhost:16686/trace/201da0f2af1b6ef9c88a7bb99e1cda39) |
| branch `c8856575` | PASS [`eb2849f8be7665a620044cdca115ec2f`](http://localhost:16686/trace/eb2849f8be7665a620044cdca115ec2f) | PASS [`1397967d9f91595ab4a57bc10f8cc685`](http://localhost:16686/trace/1397967d9f91595ab4a57bc10f8cc685) | PASS [`1e87ea699eb3e8c1ed029eb2d3ec1dfb`](http://localhost:16686/trace/1e87ea699eb3e8c1ed029eb2d3ec1dfb) |

**Sticky seat: 2×3.** The trace links name the successor's holder-stopped/publication path. Every
run completed the no-vacancy and exact hand-over sequence assertions.

| Revision | Run 1 | Run 2 | Run 3 |
|---|---|---|---|
| main `bad08475` | PASS [`f6fcdc8dc865e663e0ccabde22129718`](http://localhost:16686/trace/f6fcdc8dc865e663e0ccabde22129718) | PASS [`84b165d845e8128719c1843ac23cc1e5`](http://localhost:16686/trace/84b165d845e8128719c1843ac23cc1e5) | PASS [`c4491deda3ca92f07c9b5cfb8fe5fd2c`](http://localhost:16686/trace/c4491deda3ca92f07c9b5cfb8fe5fd2c) |
| branch `c8856575` | PASS [`e6a6504de7812a903934559058375a4b`](http://localhost:16686/trace/e6a6504de7812a903934559058375a4b) | PASS [`f725a7d792fb0d082c4260a633d0298c`](http://localhost:16686/trace/f725a7d792fb0d082c4260a633d0298c) | PASS [`9b38a8d0b3a7d6b018e55e5a68273362`](http://localhost:16686/trace/9b38a8d0b3a7d6b018e55e5a68273362) |

All 12 selected traces were retrieved from local Jaeger and saved as `selected-trace.json` beside
their run's output. Machine-readable results and preserved run evidence:
`/home/admin/rdm-lanes/e7s3/target/evidence/lifecycle-comparison/matrix.json` and its
`{main,branch}/{provider_state,sticky_seat}-{1,2,3}/` directories. Baseline cell times:
provider 25.06/24.97/25.02 s; sticky 14.13/17.14/16.96 s. Branch times:
provider 25.62/24.92/25.63 s; sticky 14.03/17.01/14.41 s. Build time is separate in each output log.

**Interpretation.** There is no branch-only RED in the requested matrix, and neither cell is RED
on main in these runs. Therefore these runs reproduce neither gate failure and establish neither
as outside. The original parallel-gate failures remain unexplained. No speculative receive-path
fix, provider-state fix, election fix, cell edit or bound change was made for those two failures.
At that checkpoint no gate was rerun and nothing was landed. Luke subsequently authorized the new `f261a198` landing after the assertion fix; it passed and was accepted.

**Receive-path review of the first suspect.**
`crates/rdm-transport/src/receive_order.rs:329–331` keys the carried version by
`(publisher.to_string(), subject mesh)`, where `PublisherId`'s string includes both path and
incarnation (`crates/rdm-core/src/publisher.rs:17–20`). Its stale comparison is strict `<`
(`receive_order.rs:398`), so the revision comparison admits an equal key, and a successor's
version 1 has its own publisher source. The additional held-projection stop check compares only
the same publisher (`membership.rs:2227–2229`). Separately, the branch's full operation identity
dedupe can suppress re-entry of the same operation from a successor publisher; the carried
revision is still judged and recorded (`membership.rs:1428–1430`). That is a changed receive
path relevant to further reproduction; the all-GREEN matrix does not establish that it caused
or could not cause the original failures.

**Precise provider-state observation for the i144 owner, from the original RED gate.**
The fixture suspends the exact runtime, waits for its os-stopped row, then calls Kill and asserts
that the test kit reported `exited=true` (`crates/rdm-test/tests/runtime/provider_state.rs:123–126`);
it proceeded to fail at line 141, so that Kill acknowledgement succeeded. The retained evidence
for `nwq5r2wm30r4/mtsdp2hc8bjy`, on 2026-10-16 UTC, is:

1. 23:25:23.006366 — os-stopped `recorded`
   ([flame](http://192.168.68.99:16687/trace/a79bbdd5cf8878c67453178e8833ae5b)).
2. 23:25:23.257674 — provider inspection verdict `running`
   ([flame](http://192.168.68.99:16687/trace/626e5724e2c5d9c15a0c14793d3cf5b6)).
3. 23:25:23.263247 — os-stopped `cleared`, detail `running`
   ([flame](http://192.168.68.99:16687/trace/df6bda8fae6d1fd7d660bf0fb1d3c691)).
4. 23:25:23.773021 — the provider's exit verdict, **509.774 ms after the clear**
   ([flame](http://192.168.68.99:16687/trace/4a15763f9dc8db1bc5911dccc191324c)).

`crates/rdm-admin/src/admin/recovery.rs:289–292` clears the os-stopped row on a Running verdict.
The saved child spans do not timestamp the parent test's Kill return; no finer ordering of that
return against the Running inspection is inferred here. The observed clear reason was running,
not exited, and the exit verdict followed about 510 ms later despite the Kill being acknowledged
as exited by the test kit. Why that inspection returned Running remains unestablished; the six
provider matrix runs did not reproduce it. Provider code and the i144 cell are untouched.

**Original sticky-seat observation, without attribution.**
`crates/rdm-test/tests/elections/sticky_seat.rs:546–551` rejects a seatless resolution during the
replace-to-ready interval. At 23:25:39.251252 UTC, `mesh1.admin.1.new` resolved mesh1 with previous
NodeId `7yyns8c7sykk` and empty winner fields
([vacancy flame](http://192.168.68.99:16687/trace/6c6c55baa54c041220d5ff72cc0aabf5));
`crates/rdm-admin/src/election.rs:406–417` emits that span. It had heard the old revision-1 seat at
23:25:39.186883 and installed the successor's revision-2 primary-updated at 23:25:41.099265
([installation flame](http://192.168.68.99:16687/trace/00c525fbf8d0dca8467d37f5e41a87a6)).
The six sticky-seat matrix runs did not reproduce this vacancy; its source and bounds are untouched.

Original gate log: `/home/admin/rust-distributed-mesh-deploy/gates-e7s3-be5102b5.log`; detailed
output is `.log.detail`, per-stem outputs are in `.log.d/`. Original evidence is preserved under
`/home/admin/rust-distributed-mesh-deploy/kept/gate-gates-e7s3-be5102b5-{provider_state,sticky_seat,rdm-transport:main}/`.
No new ruling question is raised by this comparison; R10 remains open and no e3 work has started.

#### e3 — the gossip envelope: one typed shape for what every frame declares (Luke Q20: A, 2026-10-16)

- **Goal → gap.** The receive-rules table's columns (author, subject, ordering, identity) are an
  envelope that today each of the 22 frames carries in its own shape (`publisher` as a string here, an
  exact birth there, `topology_version` on some, `forwarded_by` on most). After e2 has shown, row by
  row, which facts each frame needs, collapse them into one typed envelope and move the classifier
  onto it; close R10 by construction (a status carries its author's committed revision in `order`).
  Not before e2: a field chosen before its row proved the need is the universal-field design a1
  refused, and a second reseal would follow.
- **Standard (Luke, 2026-10-16: "just standards, CloudEvents").** The envelope is CloudEvents 1.0 with
  its Sequence extension; no RDM-named attribute on the wire. The mapping, which e3's packet freezes
  and a1 reviews first:

  | CloudEvents attribute | RDM meaning |
  |---|---|
  | `specversion` | `1.0` |
  | `id` | the delivery identity, one per frame, kept on a resend |
  | `source` | the author's exact birth as a URI, `rdm://<fabric_id>/<node_id>/<incarnation>`; a new incarnation is a new source |
  | `type` | the frame kind (`rdm.mesh.digest`, `rdm.mesh.members`, `rdm.node.deleted`, …); a reserved kind is a `type` the receiver refuses |
  | `subject` | the key the fact is about: a birth, `(mesh, publisher)`, a cohort, a fabric |
  | `sequence` (Sequence extension) | the per-`(source, subject)` version: `digest_seq`, `topology_version`, the seat `revision` |
  | `time` | rafka-time of the fact; never an ordering |
  | `data` | the body |

  Receive rule in those terms: for one `(source, subject)`, a higher `sequence` installs, equal with
  identical content is a duplicate, lower is stale, a gap is repair; a new incarnation in `source`
  supersedes the old one's facts for that subject. The page cites the CloudEvents 1.0 specification
  and its Sequence extension.
- **Shape before the standard was chosen (superseded by the table above; kept for the e2 notes):**

  ```text
  GossipFrame {
    author:       Birth,                  // the exact birth that authored it, never the forwarder
    forwarded_by: Option<Birth>,
    subject:      Subject,                // a birth | (mesh, source_publisher) | a cohort (mesh, node_type) | the fabric seat | a fabric
    order:        Ordering,               // Revision(u64) | Publication { version, snapshot_id, chunk_index, chunk_count } | Operation(identity) | Advisory
    body:         FrameBody,              // the 22 kinds, payload only
  }
  ```

- **Requirements.** R10 closed (the author's revision on `MeshStatus` / `FabricStatus`, so a delayed
  predecessor's frame is refused by revision); one classifier for all categories; every handler's
  own identity check deleted; the gossip wire changed once (`wire format moved`, mesh-audit reseal,
  every golden); no new frame kind, no counter beyond the orderings the table already names, no ack.
- **Acceptance cells.** Every e2 cell green unchanged on the envelope (the proof it changes no
  behaviour); `a_delayed_predecessors_status_is_refused_by_revision` (R10 closed);
  `the_classifier_decides_every_category_from_the_envelope_alone` (no handler reads identity);
  the recovery cell (topology read, then stale gossip of each category: no rollback) as the flame.
- **Page text.** gossip.md §3.1's catalog rows and the receive-rules table restated on the envelope's
  fields; exact old → new to Luke at e3's landing. A gossip change: Luke's permission is Q20.
- **Sizing.** One seat, after the last e2 row.
- **State.** not started.



## Verification

- [x] e1 green in the RDM gate with both flames cited in its row; the approved gossip.md receive table is on main.
- [ ] Every row of the table has a green cell; no frame kind is "taken as it arrives" without a stated rule.
- [ ] receive_order.rs has one shape per category and no contradictory check.

## Cross-references

- `docs/plans/i146-owed-messages-prd.md` (the directed-message unit this is separate from)
- `docs/plans/rulings/i143-rulings-2026-10-12-rx-authz.md` R-RX1 (data validity stays: version ordering of the same source, duplicate receipt keys, backward transitions)
- `docs/architecture/gossip.md` §3.1, §3.3, §7; `crates/rdm-transport/src/receive_order.rs`
- `docs/plans/i143-prd.md` e12.s72 (the gossip cut), for the catalog as it stands
