## Q61: Before the fabric's first state-commit, is a Build fact durable on the shared root when it is committed, or held in the scratchpad?

### 1. Spec

[build.md §5.2](../../architecture/build.md#52-rafka-adapter): "A recovery-critical Build fact (the accepted Build, an attempt claim, a step receipt, a completion) is not committed until it is durable in storage that survives the loss of the current fabric-primary. Storage local to the active fabric-primary alone is insufficient for failover. The Build facts are the lanes on the fabric's shared store root (§5.3), each written by the seat that owns it, so a successor reads them there"; "No Build is current before it is recoverably durable."

[fabric-state-sync.md](../../architecture/lifecycles/fabric-state-sync.md): "RDM writes none of its own records during state-sync; they wait in the scratchpad for state-commit." [R14 item 9](../rulings/i143-r14-rdm-shared-store.md): "Before state-commit nothing changes: rows stay in the scratchpad". [states.md, Rounds](../../architecture/lifecycles/states.md#states): "Before state-commit, RDM holds its entity rows in the scratchpad; state-commit publishes the required held rows to the configured stores. A node declared-state fact acknowledged as Applied is written to `status/<mesh>/declared` on the shared store root before the reply, including before state-commit" (Q54).

Owner: the fabric-primary (the fabric lane, `fabric/builds/`), each mesh-primary (its lane and its mesh's attempt claim). Sequence: accept → durable → current; each step's receipt is committed before the next step runs; at state-commit each node publishes what it holds.

### 2. Issue

`accepted_topology`: a node-admin the Build creates exits code 3, "the provider wrote no runtime record for this runtime within 10s"; red once (gate `ad8633d6`, 2026-10-17). On main the same wait is in 59 of the 61 Build-journal turns that held their writer over 5 s in the 8 hours to 15:40 UTC on 2026-10-18 (10–25 s each); the cell stays green when the wait falls after the runtime record is written.

### 3. Flame

Red, gate `ad8633d6`. The create of mesh1.admin.3, http://192.168.68.99:16687/trace/6a76509a005ffe60377c8216c734107e:

```
rdm.node_admin.deployment.update.via-step  22:43:36.754489  558 ms  step=DeployRuntime  io_calls=2 io_ms=556
rdm.node_admin.deployment.update.via-step  22:43:48.139618    0 ms  step=RegisterExactRuntimeHandle
rdm.node_admin.deployment.update.via-step  22:43:48.629559    0 ms  step=ResolveProviderControlDomain
rdm.node_admin.deployment.update.via-step  22:43:49.134250    5 ms  step=MakeRuntimeFactAvailableToBirth  io_calls=1
rdm.node_admin.deployment.update.via-step  22:43:49.519458    6 ms  step=WaitForBind  outcome=failed
rdm.mesh.node.create.via-deployment        22:43:37.322579 10765 ms  node=mesh1.admin.3  (the created admin's boot)
```

The fabric-primary's commit-state publication in the same window, http://192.168.68.99:16687/trace/ff1c285fa5811d1bd25056dea1b92e66:

```
rdm.node_admin.fabric.update.via-round-action  22:43:34.169900  13987 ms  busy_ns=36055680  round=commit-state rows=95
  stores=fabrics=1,fabric=1,seat-moves=2,meshes=1,nodes/mesh1/rows=5,nodes/mesh1/ids=5,nodes/mesh1/contacts=5,status/mesh1/meshes=2,status/mesh1/declared=5,status/mesh1/held-running=2,fabric/builds/rev-00000000000000000000=2,fabric/builds/rev-00000000000000000001=63,meshes/mesh1/claims=1
```

DeployRuntime ends at 37.312; the next step starts at 48.139, when the publication ends (48.157). The admin's 10 s wait ended at 48.087.

Main, lane e7s3's `accepted_topology` run (green), http://192.168.68.99:16687/trace/a19fc11f5818838afd5e42aaaa8ef3a7 and http://192.168.68.99:16687/trace/a84361c53779576399ece25e099a5340:

```
rdm.node_admin.fabric.update.via-round-action  11:58:06.860250  15724 ms  round=commit-state rows=94
rdm.node_admin.build.update.via-journal-turn   11:58:09.949206  op=append_step_receipt waited_ms=46 held_ms=11991 lane_ms=11990
```

Over the 8 hours (ClickHouse, the same process): each such turn ends within ±650 ms of the publication's end (most within 70 ms); its own blocking I/O is 0–1 220 ms.

### 4. Observations (commit `be62dec9`)

- [code] `crates/rdm-node/src/store_root.rs:104` `StoreRoot::records`: on a root holding no Fabric row every store opens held (`FileRecords::open_held`). `crates/rdm-admin/src/build_lanes.rs:214` opens each lane revision through it, `crates/rdm-admin/src/mesh_claims.rs:95` each mesh's attempt claim; `crates/rdm-admin/src/status_storage.rs:689` opens `declared` directly (Q54).
- [code] `crates/rdm-node/src/record_store.rs:598` `publish_held` takes the store's write lock and moves it into one blocking call that writes every held row, each a file fsync and a directory fsync (`write_file` `:843`, `insert_file` `:893`).
- [code] `record_store.rs:456` `insert_claimed` takes the same lock (`writer`, `:525`) before it decides held or disk. A lane append (`build_lanes.rs:337`) goes through it, and `FileJournal::append_step_receipt` (`crates/rdm-admin/src/build_state.rs:2466`) holds the journal's writer across the append.
- [code] The fabric lane's held rows pass a publish gate that drops them when the writer no longer holds the fabric seat (`build_lanes.rs:215`, `build_state.rs:2043`, `crates/rdm-admin/src/fabric_storage.rs:462`). The R-BI6 cell asserts nothing of `fabric/builds/` is on disk before state-commit (`build_state.rs:3503-3519`).
- [code] The created runtime's record is written at MakeRuntimeFactAvailableToBirth, three receipts after DeployRuntime starts the runtime; the runtime's boot waits 10 s for it.
- [flame] `ad8633d6`: the receipt after DeployRuntime waits 10.8 s, inside the publication of 95 rows (63 of them `fabric/builds/rev-…1`); the next two receipts take 490 ms and 505 ms.

#### Inferred

- [inferred] The publication's time per row (Build-carrying publications, ClickHouse) was 13 ms p50 and 52–68 ms p90 per hour from 12:00 to 18:00 UTC on 2026-10-17, and 85–100 ms p50 and 180–225 ms p90 since 09:00 UTC on 2026-10-18; the rows per publication are unchanged (p50 about 50). What raised the per-fsync cost is unread.
- [inferred] A fabric-primary lost before the fabric's first state-commit takes every Build fact it held with it; a successor reads no Build from the root (from the code above; not run).

### 5. Delta

Spec says a Build fact is not committed until it is durable on storage that survives the fabric-primary's loss (build.md §5.2); code holds Build facts and attempt claims in the fabric-primary's scratchpad until state-commit, as fabric-state-sync.md and R14 item 9 say of RDM's records, and a receipt written while they are published waits for the whole publication.

### 6. Arms

Every arm keeps one file per Build fact, written with a file fsync and a directory fsync (no store format moves).

- A: Build facts and attempt claims are written to the shared root when committed, before state-commit included: the lanes and the claims open directly as `declared` does (Q54); the fabric lane's publish gate goes (a moved writer meets the lane's seal, §5.3); the R-BI6 cell's held assertions turn; fabric-state-sync.md's line names Build facts beside the declared fact — changes the store open path (two sites), the publication (the gate goes), one doc line.
- B: Build facts stay held until state-commit: a write to a store whose publication is running is held and written by that publication, which writes until nothing is held and never waits on the disk under the write lock; build.md §5.2's durability holds from state-commit on — changes the record store's publication and build.md §5.2.
- x: something else.
