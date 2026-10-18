# Q54 — does Applied make a node declared-state row durable before state-commit?

Ruled A by Luke, 2026-10-17. Q53's permission to fix the mesh stands; this is the newly exposed conflict with R14's publication boundary. No phase boundary has been changed.

## 1. Spec

[Q53 A](../rulings/i143-q53-successor-member-status.md) requires ReadyForTraffic applied and acknowledged, seat taken, and the successor's first complete version carrying ReadyForTraffic for the same exact birth; no re-fold or wait.

[R14 item 9](../rulings/i143-r14-rdm-shared-store.md): “Before state-commit nothing changes: rows stay in the scratchpad”. At commit-state each writer publishes its held rows; subsequent writes go directly to the shared root. [States, Rounds](../../architecture/lifecycles/states.md#the-round-checklist) likewise puts required held writes at state-commit. Owner: each row's writer, the mesh-primary for these declared states. Required sequence under Q53: acknowledged Ready → successor acquires it from the shared root → complete projection. Required publication boundary under R14: private scratchpad → state-commit → shared root.

## 2. Issue

The exact-birth successor cell acknowledges Ready through StatusAuthority, then loses that writer: the published control folds 1 row and projects ReadyForTraffic; the unpublished case folds 0 and projects Pending, in both RED runs (`291f705c`, timed evidence `6e1a69ee`); the pre-commit put was acknowledged in the private scratchpad and never reached the shared root.

## 3. Flame

Observations from the product span pipeline, real FileStatusStorage, StatusAuthority and successor Records/Backbone projection:

[Unpublished case](http://localhost:16686/trace/371d28b9a8272d5934de9a6b98325460):

```text
rdm.node_admin.status.update.via-declared-put holding="true" outcome="acknowledged" state="ReadyForTraffic" node_id="892agk4g7v5c" incarnation_id="7wgn06396wfn"
```

The exact node id and incarnation and the unabridged measured spans are in the retained JSONL; the put and fold belong to this one trace. The put succeeds, the writer reads its own row, the writer and scratchpad are dropped, and the independent successor reads no row.

[Published control](http://localhost:16686/trace/b96f9ba477d979f3ec152f41aaac4753): same calls, with the existing Scratchpad::publish before the writer ends; the successor's fold has `node_declared_rows="1"`, its exact-birth projection ReadyForTraffic. The unpublished fold has `node_declared_rows="0"`, projection Pending.

Canonical log `/home/admin/rust-distributed-mesh-deploy/q53-successor-timed-red.log`; original captured spans (with measured start/end), OTLP conversion and queried Jaeger responses `/home/admin/rust-distributed-mesh-deploy/kept/q53-successor-red/`; verification `/home/admin/rust-distributed-mesh-deploy/q53-successor-timed-flames.log`. The conversion renames the captured timestamp fields; it invents no timestamp or span.

The preceding live s88 failure has the same phase exposure: the old mesh2 primary opened with `writes_to="scratchpad"`; its commit-state call ended Indeterminate; its replacement opened `writes_to="store-root"` and [folded zero rows](http://localhost:16686/trace/15cb9cd91901c07ab8111839908d878b). Those older spans do not instrument the exact put's holding flag; the deterministic cell supplies that observation, rather than treating absence of a publication span as proof.

## 4. Observations (commit `6e1a69ee`)

- [code] `crates/rdm-admin/src/status_storage.rs:687`: `declared` opens through `root.records`, therefore uses the root's scratchpad before first publication.
- [code] `crates/rdm-node/src/store_root.rs:104`: `records` opens a held FileRecords when the root has a scratchpad. `record_store.rs:370` holds the put and returns Ok, with no filesystem write.
- [code] `crates/rdm-admin/src/status_rpc.rs:1130`: `persist_then_commit` accepts that successful put, updates the applied state, then returns Applied. The acknowledgement is real, but it does not promise a disk write before publication.
- [flame] The put is `holding=true, outcome=acknowledged`; the successor folds zero. In the publication control the successor folds one and its first projection is ReadyForTraffic.
- [code] `crates/rdm-admin/tests/status_seat_fold.rs:337`: the new cell uses an independently held writer root and successor disk root, the same exact RPC birth and no replacement report. No removal, wrong name or revision is introduced.
- [code] `crates/rdm-admin/src/status_rpc.rs:224`: SeatFold reads the declared rows correctly from the successor's store. There is nothing to fold in the unpublished case; repeating or waiting on that read cannot recover the lost private row.

### Inferred

- The original live zero-row read is consistent with this loss of a private scratchpad. The older trace alone cannot exclude every other cause of its missing row. The RED establishes that Q53's unqualified acknowledged-state guarantee is impossible under R14 item 9 when this writer ends before publication.

## 5. Delta

Spec says an acknowledged member state is carried to its successor by the shared root; code has no such path before the predecessor publishes its private scratchpad.

## 6. Arms

Every arm assumes Q53's mesh-change permission stands and no re-fold, wait, fallback copy or invented Ready status is added.

- **A:** Make NodeDeclaredRow an acknowledged durable fact: open the existing `status/<mesh>/declared` store directly on the shared root, and complete its write before Applied, including before state-commit. Same row, same key/path/format, no second copy. This explicitly excepts these facts from R14 item 9; other stores keep their phase boundary. Wire: unchanged; publisher: existing mesh-primary; timer: unchanged; tier: unchanged; fork: none. The exact page line below lands with the code. This makes both Q53 acquisition cases carry Ready.
- **B:** Keep R14's publication boundary and qualify Q53's first-projection guarantee to published states. A pre-commit Applied is a private held acknowledgement, not durable acquisition evidence. The living birth's existing eligible declaration to its new exact authority must restore its state; any missing declaration still needs its own read and fix. Wire: unchanged; publisher: existing mesh-primary; timer: no new timer; tier: unchanged; fork: none. This changes the guarantee and does not alone dispose of the live 60 s setup red.

### Exact approved page line for A

`docs/architecture/lifecycles/states.md`, Rounds:

Old:

> Before state-commit, RDM holds its rows in the scratchpad; state-commit publishes the required held rows to the configured stores.

New:

> Before state-commit, RDM holds its entity rows in the scratchpad; state-commit publishes the required held rows to the configured stores. A node declared-state fact acknowledged as Applied is written to `status/<mesh>/declared` on the shared store root before the reply, including before state-commit; a successor acquires that fact without creating a new lifecycle transition.

This ruling overrides R14 item 9 for that one store; the exact page line lands with the implementation. The fold ignores rows for births absent from its incoming view. Packet format: [ruling-packet skill](/home/admin/rafka-v2/.claude/skills/ruling-packet/SKILL.md); the need for this decision comes from the two quoted contracts, not an additional approval rule in the skill.
