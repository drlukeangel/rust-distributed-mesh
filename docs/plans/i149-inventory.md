# i149 inventory: storage handles, write paths, claims, finish-writes, copy facts

> Read at 7d0b316eb against storage.md §2 as it stood then (freeze, tails, decision, release). Storage §2 is now block, finish outstanding writes, copy, switch, unblock (f3b9be06f): §7's "§2 Freeze" references map to Block and Finish outstanding writes, the pending log is gone (connection events stay owed on the Q169 queue), and the decision is the `commit` record alone. The code facts are unchanged.

Read at `origin/main` **7d0b316eb**. Every citation is `file:line` at that SHA, paths from the
repository root. Read-only: no code, no build, no commit. A deduction not read directly in code is
marked **[inferred]**. Test modules (`#[cfg(test)] mod tests`) are excluded throughout.

Spec read: `docs/architecture/storage.md` §2 (54-172); plan `docs/plans/i149-topology-storage-provider.md`
e2 (231-249) and e3 (250-268).

Counts: §1 creation points 9; owner rows 55 (O1-O47 admin, T1-T5 tasks/closures, N1-N3 node) and
2 aliases (C5, `mesh_claims`); §2 write-path rows 21 with 60+ call sites; §3 claim operations 12;
§4 finish-writes: 1 covered set, 10 gaps; §5 copy facts 22; §6 cross-writer reader rows 14 (about
100 call sites); §7 conflicts 16.

---

## 1. Where a storage handle is created, and every owner that keeps one

### 1a. Creation points

| # | file:line | what is created | store / place |
|---|---|---|---|
| C1 | `crates/rdm-admin/src/storage_backend.rs:127` | `StoreRoot::open(store_dir, fabric_id)`: the per-fabric root, created with its `layout` marker when empty (`crates/rdm-node/src/store_root.rs:138-156`, `:94-116`) | root dir |
| C2 | `crates/rdm-admin/src/storage_backend.rs:129` | `FileFabricStorage::open` → `names: FileRecords` at `<store_dir>/fabrics/` (`fabric_storage.rs:218`, `store_root.rs:195-197`) + `records: StoreLog` on `fabric.jsonl` (`fabric_storage.rs:219`) | fabric |
| C3 | `crates/rdm-admin/src/storage_backend.rs:130` | `FileMeshStorage::open`: `StoreLog` on `meshes.jsonl` + `StoreLocks` on `locks/` (`storage.rs:367-371`) | meshes + locks |
| C4 | `crates/rdm-admin/src/storage_backend.rs:131` | `FileNodesStorage::open`: `StoreLog::open_observed` on `nodes.jsonl` with the path-index observer + `StoreLocks` (`storage.rs:1126-1140`) | nodes + locks |
| C5 | `crates/rdm-admin/src/storage_backend.rs:133` | `root_runtimes = Some(nodes.clone())`: **an alias of C4**, not a second handle | nodes |
| C6 | `crates/rdm-admin/src/storage_backend.rs:134` | `FileJournal::open_on_root`: `StoreLog::open_observed` on `builds.jsonl` + `contexts: FileRecords` in the **data dir** `attempt-context/` (`build_state.rs:2410-2437`) | builds + data dir |
| C7 | `crates/rdm-admin/src/storage_backend.rs:146-152` | `OpenConnections` closure capturing `root` and the `LogWriter`; each call opens a **new** `FileConnectionsStorage` (its own `StoreLog` and append mutex) on `connections.jsonl` (`crates/rdm-node/src/storage.rs:119-122`) | connections |
| C8 | `crates/rdm-admin/src/storage_backend.rs:117-123` | `own_row` / `put_own_row`: a fresh `FileRecords` on `<data_dir>/nodes/` per call (`storage.rs:1144-1152`), key `self` | data dir |
| C9 | `crates/rdm-node/src/storage_backend.rs:35-42` | node binary: `StoreRoot::open` + `FileConnectionsStorage::open` with `LogWriter::new(node_id, incarnation)` | connections |

Memory backend (no shared rows): `crates/rdm-admin/src/storage_backend.rs:188-202`,
`crates/rdm-node/src/storage_backend.rs:57-59`. No other production code opens a `StoreRoot`,
`StoreLog`, `StoreLocks` or `FileRecords` (grep of `StoreRoot::open|at`, `StoreLocks::at`,
`FileRecords::open`, `store_log::read_file|latest_in|keys_in` outside test modules finds only the
sites above and the constructors they call).

### 1b. The admin's boot: the one destructure and the first clones

`crates/rdm-admin/src/admin/boot.rs:380` destructures `StorageBoundaries` into `fabric_backed`,
`mesh_storage`, `nodes_storage`, `root_runtimes`, `open_connections`, `journal`, `finish_writes`.
Then:

| local | file:line | obtained from |
|---|---|---|
| `fabric_storage` | `admin/boot.rs:385` | `wiring::apply(wiring.fabric_storage, fabric_backed)`: an application decorator may wrap it (`crates/rdm-admin/src/wiring.rs:181-182`) |
| `attempt_contexts` | `admin/boot.rs:386` | `AttemptContexts::on(journal.clone())` |
| `mesh_claims` | `admin/boot.rs:389` | `mesh_storage.clone()`: **an alias** of the same `FileMeshStorage` |
| `connections_storage` | `admin/boot.rs:605` | `open_connections(name)`: one C7 instance for the admin |
| `builds` | `admin/boot.rs:1074-1078` | `FabricBuildStateAdapter::join(…, journal.clone(), …)` |
| `builds_dyn` | `admin/boot.rs:1083` | `FramedBuilds::new(wiring::apply(wiring.builds, builds.clone()), runs)`: application decorator again (`wiring.rs:185`) |
| `caches` | `admin/boot.rs:384` | `Caches::default()`: the writers' object copies, not a handle (`record_cache.rs:55-63`) |

**Two parallel paths reach the one `FileJournal`:** directly as `journal` (C6) and through
`FabricBuildStateAdapter.local` → app decorator → `FramedBuilds` → `builds_dyn`. Both are cloned into
owners below.

### 1c. Owners that keep a handle beyond a call (rdm-admin)

R = reads the store, W = writes it (from the call sites in §2 and §6). "boot" = where the owner is
filled in `crates/rdm-admin/src/admin/boot.rs`.

| # | owner type.field | defined at | store | R/W | filled at (boot) |
|---|---|---|---|---|---|
| O1 | `StorageBoundaries` (all fields) | `storage_backend.rs:50-73` | all | – | `:380` (consumed) |
| O2 | `AcceptedStore.storage` | `accepted.rs:1380` | fabric | R+W (`accepted.rs:1395`, `:1424`) | `:1038` |
| O3 | `AttemptContexts.log` | `build_claim.rs:56` | builds (data-dir contexts) | R+W (`build_claim.rs:77`, `:87`) | `:386` |
| O4 | `ControlPlane.builds` | `control_plane.rs:26` | builds | R+W (`control_plane.rs:302,339,525,560,1007`) | `:1197-1213` |
| O5 | `ControlPlane.claims` | `control_plane.rs:70` | in-flight locks | R+W (`:218,314,345,924,938`) | `:1211` |
| O6 | `ControlPlane.node_rows` (OnceLock) | `control_plane.rs:83` | nodes | R (`:407`) | `:1559` |
| O7 | `ClaimDoor.builds` / `.claims` | `build_claim.rs:200`, `:204` | builds, in-flight locks | R+W (`build_claim.rs:146,162,169`) | `:1216-1224` |
| O8 | `FabricBuildStateAdapter.local` | `fabric_builds.rs:179` | builds | R+W (`fabric_builds.rs:541-662`) | `:1074-1078` |
| O9 | `FramedBuilds.inner` | `build_run.rs:268` | builds | pass-through R+W (`build_run.rs:295-409`) | `:1083` |
| O10 | the per-mesh adapter struct at `build_run.rs:427`, field `.inner` | `build_run.rs:428` | builds | pass-through R+W (`build_run.rs:454-515`) | handed to a mesh's drift attempt run (`build_run.rs:420-425`) |
| O11 | `HeldRun.builds` | `build_run.rs:535` | builds | W (`build_run.rs:555`) | per run |
| O12 | `RunDoor.builds` / `.local` | `build_run.rs:609`, `:616` | builds | R+W; `local` only `absorb_facts` (memory, `build_run.rs:798`) | `:1534-1542` |
| O13 | `DriveEnv.builds` | `build_drive.rs:656` | builds | R+W (`build_drive.rs:1488,1497,1584`; release `:986,1503`) | `:1543-1556` |
| O14 | `BuildExecutor.builds` | `executor.rs:410` | builds | R+W (`executor.rs:489,587,601`) | `:1533` |
| O15 | `LocalVerdicts.local` | `build_op.rs:892` | builds | memory only (`build_op.rs:898` `absorb_facts`) | `:1555` |
| O16 | `FetchBuild.local` | `fetch_build.rs:180` | builds | memory only (`fetch_build.rs:283`) | `:1964` |
| O17 | `StatesEnv.builds` | `fabric_status.rs:57` | builds | R (`fabric_status.rs:156,292,942…`) | `:1397-1407` |
| O18 | `BuildFactsDoor.builds` | `build_facts_read.rs:33` | builds | R (`build_facts_read.rs:72,91`) | `:1093` |
| O19 | `HeardAgain.builds` | `heard_again.rs:38` | builds | R (`heard_again.rs:128`) | `:1525` |
| O20 | `AdminSession.builds` / `.local` | `admin/session.rs:308`, `:316` | builds | R; local absorb | `:2194-2216` |
| O21 | `AdminFloor.builds` | `admin/session.rs:638` | builds | R | `:1960-1962` |
| O22 | `EntryState.builds` | `admin/session.rs:818` | builds | R | `:1677-1681` |
| O23 | `RecoverySubmitter.nodes_storage` / `.builds` | `admin/recovery.rs:14`, `:25` | nodes, builds | R+W (`admin/recovery.rs:66,88,284,547`) | `:2267-2276` |
| O24 | `AdminRunner.builds` / `.local` | `admin/runner.rs:35`, `:38` | builds | R+W | `:1485-1498` |
| O25 | `AdminRunner.connections` | `admin/runner.rs:66` | connections | R (passed to `TickleClient`, `admin/runner.rs:1263`) | `:1489` |
| O26 | `AdminRunner.nodes_storage` / `.mesh_storage` / `.fabric_storage` | `admin/runner.rs:82,85,87` | nodes, meshes, fabric | R+W (`admin/runner.rs:412,530,825,1048`; mesh delete paths `mesh_delete.rs:883,910,1805`) | `:1490-1492` |
| O27 | `Joins.runtimes` | `join.rs:187` | nodes (runtime rows) | R+W (`join.rs:224,231`) | `:656` |
| O28 | `JoinDoor.nodes` (+ `row` closure) | `join.rs:352`; closure `admin/boot.rs:1744-1749` | nodes | R+W (`join.rs:463-467`; `node_rows::record_at`) | `:1690-1750` |
| O29 | `NodeRows.storage` | `node_rows.rs:57` | nodes + name locks | R+W (`node_rows.rs:526,567,586,596,688,813`) | `:1560` via `records.set_rows` |
| O30 | `Records.rows` (OnceLock<NodeRows>) | `admin/records.rs:365` | nodes | as O29 | `:1560` |
| O31 | `Records.contacts` (OnceLock) | `admin/records.rs:289` | nodes | R | `:1167` |
| O32 | `Records.status` (OnceLock of the status fold struct at `status_rpc.rs:226`) → fields at `status_rpc.rs:229-231` | `admin/records.rs:375`; `admin/records.rs:512-514` | nodes, meshes, fabric | R (`status_rpc.rs:215-217,306-307`) | `:1072` |
| O33 | the status struct at `status_rpc.rs:448`: `.nodes_storage` / `.meshes_storage` / `.fabric_storage` | `status_rpc.rs:474,479,482` | nodes, meshes, fabric | R+W (`status_rpc.rs:700,716,746,1061,1135,1156,1894`) | `:1318-1327` (a OnceLock slot, set at `:1318`) |
| O34 | the same struct's `.builds` | `status_rpc.rs:543` | builds | R (`status_rpc.rs:958`) | same |
| O35 | `MeshRowsWriter.nodes` / `.meshes` | `admin/rows_writer.rs:50`, `:51` | nodes, meshes, name locks | R+W (`admin/rows_writer.rs:101,215,219,254,259,339`) | `:1663` |
| O36 | `FabricRowsWriter.storage` | `admin/rows_writer.rs:449` | fabric (+ name index) | W (`admin/rows_writer.rs:426,469`) | `:1055`, moved into the task at `:1058-1067` |
| O37 | struct at `admin/reconcile.rs:7` field `.nodes` / `.meshes` | `admin/reconcile.rs:22`, `:24` | nodes, meshes | R (`admin/reconcile.rs:107,124`) | `:1549` |
| O38 | `ProviderDeparture.nodes` | `admin/reconcile.rs:171` | nodes | R (`admin/reconcile.rs:146,196`) | `:1550` |
| O39 | `Renewals.nodes_storage` | `cert_renewal.rs:138` | nodes | R+W (`cert_renewal.rs:180,303,329,497`) | `:2589-2594` |
| O40 | `Looker.nodes` / `.connections` (struct at `:134`) | `seat_watch.rs:146`, `:150` | nodes, connections | R (`node_investigation.rs:161`; `seat_watch.rs:158` → `TickleClient`) | `:1479`, `:2296-2304`, `:2628` |
| O41 | `TickleClient.connections` | `offline.rs:330` | connections | R (`offline.rs:366` `connections_to`) | `:2419` and per tick |
| O42 | `ProbeAnswers.nodes` / `.meshes` | `admin/adoption.rs:530-531` | nodes, meshes | R (`admin/adoption.rs:556,566`) | `:738-745` |
| O43 | the app's `wiring.fabric_storage` decorator | `wiring.rs:181-182` | fabric | **outside RDM** | `:385` |
| O44 | the app's `wiring.builds` decorator | `wiring.rs:185` | builds | **outside RDM** | `:1083` |
| O45 | the admin's `ConnectionsWriter.Inner.storage` | `crates/rdm-node/src/connections_writer.rs:92` | connections | R+W | `:640-644` |
| O46 | `FinishWrites` closure | `storage_backend.rs:135-145` | meshes, nodes, fabric | waits only | captured at open; stored in the update struct at `fabric_handover.rs:134`, filled `:1478` |
| O47 | `OpenConnections` closure | `storage_backend.rs:146-152` | connections | opens | `:380`, called `:605` |

Closures and spawned tasks that capture a handle (owners with no struct):

| # | file:line | captures | does |
|---|---|---|---|
| T1 | `admin/boot.rs:1055-1068` | `FabricRowsWriter` (fabric) | sweep task, W on every primary change |
| T2 | `admin/boot.rs:1375-1394` | `mesh_storage` in the `RecordMesh` closure (OnceLock `declarer.record_mesh`) | W `update_mesh` |
| T3 | `admin/boot.rs:1758-1770` | `nodes_storage`, `mesh_storage` in `StoredSource` | R |
| T4 | `admin/boot.rs:1789-1818` | `nodes_storage` in the runtime-rows task | R+W `put_runtime` |
| T5 | `admin/boot.rs:1844` (task), `:2324` (silence), `:2342` (ladder), `:2593` (renewals), `:2966-2998` (drift) | `nodes_storage`, `connections_storage`, `attempt_contexts`, `builds_dyn` | R, and W through their owners |

### 1d. rdm-node (ordinary node)

| # | owner | file:line | store | R/W |
|---|---|---|---|---|
| N1 | `NodeStorage.connections` (dropped after boot) | `crates/rdm-node/src/storage_backend.rs:14`; opened `node.rs:468` | connections | – |
| N2 | `decorate_storage(base_storage)` (the app's decorator, outside RDM) | `crates/rdm-node/src/node.rs:470-473` | connections | wraps |
| N3 | `ConnectionsWriter.Inner.storage` | `crates/rdm-node/src/connections_writer.rs:92`, set once at `:166-177` | connections | R (`:267`, `:295`) + W (`:420-421`, `:439-444`) |

`ConnectionsWriter` is shared as `connections` into the registry, the data-plane client observer, the
recovery dial closure and `NodeSeams` (`node.rs:484`, `:501`, `:507-509`, `:527`, `:538`, `:702`); none
of those holds the storage directly.

---

## 2. Write paths per store

Durability of the primitives:
- `StoreLog::append` (`crates/rdm-node/src/store_log.rs:611-623`): one `write` on an `O_APPEND`
  descriptor, then `fsync`, success only after the fsync (`:724-741`). A failed fsync leaves the line
  pending, reconciled before the store's next write (`:740`, `:763-773`).
- `FileRecords::write` (`crates/rdm-node/src/record_store.rs:179-189`): temp + fsync + rename + dir
  fsync (`durable_file::write_file`).
- `StoreLocks::take` (`store_log.rs:884-900`) → `record_store::insert_file` (`record_store.rs:249-262`):
  temp + fsync + `hard_link` + dir fsync. `StoreLocks::release` (`store_log.rs:912-931`) →
  `remove_file` + dir fsync (`record_store.rs:241-247`).
- Memory backend: in-process maps, nothing durable.

Unless stated, every caller below awaits the write before it answers its own caller.

| store | trait method → file impl | call sites (production) | awaited to durability before the caller is answered? |
|---|---|---|---|
| fabric.jsonl | `put_fabric` → `fabric_storage.rs:243-245` | `fabric_storage.rs:86` (in `claim_identity` ← `accepted.rs:1424`, `admin/rows_writer.rs:426`); `Caches::update_fabric` `record_cache.rs:106` ← `admin/boot.rs:1143`, `status_rpc.rs:700` | yes |
| `<store_dir>/fabrics/<name>.json` | `put_name_claim` → `fabric_storage.rs:249-251` (`FileRecords::write`) | `fabric_storage.rs:79` ← `claim_identity` ← `claim_fabric_name` `admin/rows_writer.rs:414` ← `admin/boot.rs:1137`, `admin/rows_writer.rs:469`; `accepted.rs:1424` | yes |
| same | `remove_name_claim` → `fabric_storage.rs:252-254` (`FileRecords::remove`) | `fabric_storage.rs:98` ← `mesh_delete.rs:1805` | yes |
| meshes.jsonl | `put_mesh` → `storage.rs:394-396` | `storage.rs:156` (`claim_mesh` ← `admin/rows_writer.rs:259`, `mesh_delete.rs:883`); `record_cache.rs:83` (`update_mesh` ← `admin/boot.rs:1383`, `status_rpc.rs:746`) | yes |
| locks/name (mesh) | `claim_mesh_name` → `storage.rs:400-402`; `release_mesh_name` → `:403-405` | `storage.rs:142,146,147,158` | yes |
| locks/in-flight | `take_in_flight` → `storage.rs:406-414` | `build_state.rs:1185` (default `claim_in_flight`, reached from `build_claim.rs:146` via `build_run.rs:346,485`, `fabric_builds.rs:607`); `admin/adoption.rs:251,334` | yes |
| same | `release_in_flight` → `storage.rs:415-418` | `control_plane.rs:314,345,938`; `build_claim.rs:162,169`; `build_drive.rs:986,1503`; `build_op.rs:422`; `build_run.rs:1005`; `fabric_recovery.rs:103`; `admin/adoption.rs:282,330,358,452,506` | awaited; **outcome discarded** at `admin/adoption.rs:282,358` |
| nodes.jsonl (node record) | `put_node` → `storage.rs:1205-1207` | `node_rows.rs:186` (`WrittenRows::put`), `:969` (`release_record`); every `WrittenRows::update` (`node_rows.rs:201`): `admin/boot.rs:981`, `admin/recovery.rs:284`, `admin/rows_writer.rs:101`, `admin/runner.rs:825`, `cert_renewal.rs:303,497`, `join.rs:467`, `mesh_delete.rs:910`, `node_rows.rs:596`, `status_rpc.rs:1061,1135,1156,1894` | awaited; **outcome discarded** at `admin/recovery.rs:284`, `admin/runner.rs:825` |
| nodes.jsonl (tags) | `index_tags` → `storage.rs:1212-1220`: one append per label | `storage.rs:919` ← `write_index_moves` ← `node_rows.rs:190` | awaited; a failure keeps the moves owed **in memory** (`node_rows.rs:191-193`, field `node_rows.rs:91`) |
| nodes.jsonl (path) | `index_path` → `storage.rs:1209-1211` | file backend: no write (the index is the fold) | – |
| nodes.jsonl (runtime) | `put_runtime` → `storage.rs:1175-1180` (reads latest first, skips an equal row) | `admin/boot.rs:395,517,522,523,1598,1603,1808`; `join.rs:224` | awaited; at `admin/boot.rs:1808-1813` a refusal is logged and the row retried only on the next birth change |
| locks/name (node) | `claim_node_name` → `storage.rs:1240-1249`; `release_node_name` → `:1251-1254` | `node_rows.rs:941,947,948,972,1014,1310,1338` | yes |
| connections.jsonl | `put_connection` → `crates/rdm-node/src/storage.rs:169-171` | `connections_writer.rs:421` (`persist` ← `record` `:311-318`, `settle_owed` `:513-544`); `:439`, `:444` (`land` ← `write_owed` `:605-675`) | `record`/`settle_owed`: yes. Observed rows (`record_data_plane` `:323-325`, `observed` `:598-600`, `write_drops` `:393-405`) are **queued and never awaited by the issuer** (`owe` `:342-346`; consumer task `:184-189`) |
| connections.jsonl | `append_history` → `storage.rs:189-191` | file backend: no write (`put_connection`'s line is the history) | – |
| connections.jsonl | `remove_connection` → `storage.rs:179-187` (read latest, append `removed`) | no production caller found | – |
| builds.jsonl | `FileJournal::append` → `build_state.rs:2584-2594` | `publish_accepted` `:2600-2607`, `open_attempt` `:2609-2623`, `claim_attempt` `:2654-2670`, `append_step_receipt` `:2682-2686`, `append_attempt_receipt` `:2688-2692`, `forget` `:2714-2718` | yes; through `FabricBuildStateAdapter` the local append commits before the gossip publish (`fabric_builds.rs:541-662`, publish `:516`) |
| builds (callers) | – | `accepted.rs:1429-1432`; `admin/adoption.rs:274,276,352,451,484,503,505`; `admin/boot.rs:1145`; `admin/mesh_delete.rs:703`; `admin/reconcile.rs:609`; `admin/rows_writer.rs:339`; `build_drive.rs:1488,1497,1584`; `build_op.rs:377,415,418`; `build_run.rs:555`; `control_plane.rs:302,339,525,560,1007`; `deployment/pipeline.rs:1365`; `executor.rs:587,601`; `fabric_recovery.rs:159`; `mesh_delete.rs:180,285,458,838,1776,1809` | awaited; **outcome discarded** at `build_op.rs:415` |
| builds (memory only) | `adopt_claim` `build_state.rs:2672-2680`, `absorb_facts` `:2113-2125` | `executor.rs:489`; `fabric_builds.rs:342` (gossip), `fetch_build.rs:283`, `build_op.rs:898`, `build_run.rs:798`, `mesh_delete.rs:639` | no durable write |
| data dir: attempt contexts | `put_attempt_context` → `build_state.rs:2127-2129` (`FileRecords::write`) | `build_claim.rs:77` ← `build_claim.rs:98`, `admin/boot.rs:1144` | yes |
| data dir: own row | `put_own_row` → `storage_backend.rs:121-123` → `storage.rs:1150-1152` | `admin/boot.rs:1638` | yes |
| data dir: node config / properties | via `finish_release` (name release) | `node_config.rs:135`, `node_properties.rs:108` | awaited; **outcome discarded** |

---

## 3. Insert-and-fail claim operations

**The task names `record_store insert_claimed`: no such function exists** (grep of `insert_claimed`
over `crates/`: no match). The one insert-and-fail primitive on the file provider is
`record_store::insert_file` (`crates/rdm-node/src/record_store.rs:249-262`: temp, fsync,
`hard_link`, which fails `AlreadyExists` when the key stands, dir fsync), reached through
`StoreLocks::take` (`crates/rdm-node/src/store_log.rs:884-900`). `FileRecords::insert`
(`record_store.rs:194-204`) uses the same primitive and has **no production caller**.

Lock content (`crates/rdm-admin/src/storage.rs:284-295`): `{format, record, by: (node_id, incarnation)}`.
Release re-reads the content under an `flock` on `locks/name/<name>.reclaim` (name locks only) and
removes the file only while the content names the expected claimant (`store_log.rs:912-931`, anchor
`:915`, `NameMutex` `:953-970`). A take that meets a lock and then finds it gone refuses by name
(`store_log.rs:896`).

| # | claim | primitive | file:line |
|---|---|---|---|
| K1 | mesh name | `StoreLocks::take(Name, name)`, format `mesh-name/1` (`storage.rs:267`); same mesh re-taking answers `Ok` | `storage.rs:327-335` (via `claim_mesh_name` `:400-402`); release `:337-340` |
| K2 | mesh name take-over over a Dead mesh | claim → read record → release other → claim again: **four store operations in the writer** | `storage.rs:136-161` |
| K3 | node path.name | `StoreLocks::take(Name, name)`, format `node-name/1` (`storage.rs:1073`); **same `locks/name/` directory as K1** (`store_log.rs:826`, `store_root.rs:16`) | `storage.rs:1240-1249`; read `:1232-1237`; release `:1251-1254` |
| K4 | node name take-over over a Deleted/Withdrawn record | claim → read record → release → claim | `node_rows.rs:938-950` (`take_name`); callers `node_rows.rs:209,929,1071,1301`, `take_inherited_name` `:1151`, `claim_day0_name_in_boot` `:1212`, `admin/adoption.rs:464-473` |
| K5 | node record withdraw / delete with name release | append `Withdrawn`/`Deleted` record, then release lock | `node_rows.rs:957-976` (`release_record`), `:994-1019` (`write_deleted`) |
| K6 | mesh in-flight row | `StoreLocks::take(InFlight, mesh)`, format `mesh-in-flight/2` (`storage.rs:271`); `InFlight` `mesh_claims.rs:24`, `same_attempt` `:44`, `same_claim` `:51`, `Taken` `:66` | `storage.rs:406-414`; release `:415-418`; read `:419-424` |
| K7 | attempt claim with in-flight row | default `claim_in_flight`: `take_in_flight` **then** `claim_attempt`, two operations; `FileJournal` does not override it | `build_state.rs:1184-1189` |
| K8 | Build accepted ("Insert-and-fail on its id", trait doc `build_state.rs:1123`) | in-process turn mutex `build_state.rs:2444-2474`, read tail, `accepted_conflict` `:1890`, then `append` | `build_state.rs:2600-2607` |
| K9 | attempt open ("Insert-and-fail on (build_id, attempt)", `build_state.rs:1125`) | same turn, `admit_open` `:1937`, append | `build_state.rs:2609-2623` |
| K10 | attempt claim ("Insert-and-fail on (build_id, attempt)", `build_state.rs:1169`) | same turn, the claimant check `:1952`, `not_open` `:1904`, append; process-wide gate `fabric_builds.rs:235-275` around it | `build_state.rs:2654-2670`; `fabric_builds.rs:587-598` |
| K11 | fabric name (`<store_dir>/fabrics/<name>.json`) | `name_claim` read, then `put_name_claim` = `FileRecords::write` (replace): **read-then-write, not insert-and-fail** | `fabric_storage.rs:73-89`, `:246-251` |
| K12 | store root and layout marker | `create_root` rename of a marked temp dir (`store_root.rs:94-116`); `StoreRoot::at` marker `create_new` (`:208-214`) | `store_root.rs:138-156` |

No lock under `locks/` other than `name/` and `in-flight/` (`store_log.rs:822-837`). The per-object
"hold" of a node, mesh or Fabric record is an **in-process** lock on the writer's cache
(`record_cache.rs:47-50`, `node_rows.rs:86-91`), not a store claim.

---

## 4. `finish_writes` / `FinishWrites`

- Type: `crates/rdm-admin/src/fabric_handover.rs:63`; carried at `:134`; filled `admin/boot.rs:1478`;
  the only caller is the primary's update before it calls its successor, `fabric_handover.rs:366`
  (node-primary-update step 3).
- File backend (`storage_backend.rs:135-145`) waits, in order, for `meshes.finish_issued_writes()`
  (`storage.rs:375-377`), `nodes.finish_issued_writes()` (`storage.rs:1156-1158`),
  `fabric.finish_issued_writes()` (`fabric_storage.rs:225-228`: the name-index `FileRecords` mutex and
  the `fabric.jsonl` `StoreLog`).
- What each wait is: `StoreLog::finish_issued_writes` takes and drops the append mutex once
  (`store_log.rs:637-639`); `FileRecords::finish_issued_writes` the write mutex once
  (`record_store.rs:222-224`). Tokio's mutex is FIFO, so the wait covers appends already queued on the
  mutex [inferred from tokio's documented fairness].
- Memory backend: a no-op (`storage_backend.rs:191`).

Not covered:

| # | gap | file:line |
|---|---|---|
| F1 | `builds.jsonl`: `FileJournal.file` is never waited for; nor its turn mutex | `build_state.rs:2287`, `:2311` |
| F2 | attempt contexts `FileRecords` (data dir) | `build_state.rs:2302` |
| F3 | `connections.jsonl`: the admin's `FileConnectionsStorage` is opened per call by C7 and is not in the closure; `FileConnectionsStorage::finish_issued_writes` exists and has **no caller** | `storage_backend.rs:146-152`; `crates/rdm-node/src/storage.rs:160-162` |
| F4 | the `ConnectionsWriter` owed queue (in-memory mpsc); the only wait is `observations_landed` | `connections_writer.rs:168-189`, `:342-346`, `:251-262` |
| F5 | every lock take/release (`StoreLocks` holds no mutex at all) | `store_log.rs:884-931` |
| F6 | an indeterminate pending append (failed fsync) is **not reconciled** by the wait: it takes the mutex and drops it; `reconcile` (`:626-634`) is not called | `store_log.rs:637-639`, `:740` |
| F7 | operations admitted but not yet at the mutex: a `Caches::update_*`/`WrittenRows::update` between its read and its put, a task between turns; nothing closes admission | `record_cache.rs:70-87`; `node_rows.rs:201` [inferred] |
| F8 | multi-operation sequences mid-way: K2, K4, K5, K7, tag moves after the record (`node_rows.rs:184-195`) | as cited |
| F9 | the own-row `FileRecords` (data dir) | `storage.rs:1150-1152` |
| F10 | the ordinary node: no finish-writes at all | `crates/rdm-node/src/storage_backend.rs:12-17` |

The closure captures the concrete `Arc`s at open time (`storage_backend.rs:136-138`): a replaced
handle would not be the one it waits for.

---

## 5. Copy-relevant store facts

**Line and writer identity**
1. Line: `\n{"len":N,"op":{"node_id","incarnation_id","seq"},"format":"<kind>/<n>","key","record":{…},"crc32":"xxxxxxxx"}\n` (`store_log.rs:6-23`; `encode_object` `:278-290`; `Envelope` `:306-316`, `deny_unknown_fields`).
2. `Op` = writer node id + incarnation + `seq` per writer per file (`store_log.rs:63-72`). `StoreLog::append` stamps the op from the caller's `LogWriter` (`:611`, `next_op` `:703-710`): **there is no append that takes an existing op**, so re-appending a line through it re-stamps the copier's identity. `encode_line(op, …)` (`store_log.rs:293-295`) is public and takes an existing `Op`, so the bytes of an op-preserving line can be built today; no `StoreLog` method appends them.
3. `seq` resumes past the greatest of the writer's seq in the fold and the process-global `issued()` map keyed by `(file path, writer)` (`store_log.rs:438-443`, `:703-710`). The global is outside every owner and keyed by path.
4. A retry of one write keeps its op; reconcile finds a pending line by its exact object bytes from the pre-write offset (`store_log.rs:760-812`).

**Order and "latest is current"**
5. The only cross-writer order is file position, serialized by the kernel's `O_APPEND` (`store_log.rs:25-26`, `:45-46`).
6. The fold keeps, per `(kind, key)`, the last line in file order (`Fold::take` `store_log.rs:426-435`: `latest.insert` overwrites); same for `latest_in` (`:240-241`) and `read_record` (`:205-209`). `lines()` reads the raw log from byte 0 (`:597-601`).
7. A line that fails len, crc or format refuses the open or tail read by name with its offset; nothing older is answered (`store_log.rs:35-43`, `verify` `:320-345`, `read_whole` `:666-699`).

**How readers fold, beyond latest-per-key**
8. Nodes path index: an observer maps `name → node_id` from every `node-record` line in file order, so the latest line **across different node keys** decides who is at a name (`storage.rs:1128-1136`; read `node_at` `:1190-1194`). Per-key order alone does not preserve it.
9. Builds: lines are queued by an observer (`build_state.rs:2412-2418`), sorted by `StoredFact.seq` within each read (`:2519`), and the **first** line per key wins (`seen` `:2293`, `:2526`); `next_seq` is raised past every seq read (`:2530`). `StoredFact.seq` (`:2252-2255`) is a second, record-level order distinct from `op.seq`, issued from a per-process counter (`:2589`). `stored_accepted` instead reads the latest line of its key (`:2630-2637`).
10. Builds are read from the root only while `reads_root()` holds (`build_state.rs:2439-2441`, `:2549-2551`, `:2563`), set at `admin/boot.rs:1049`; otherwise the fold comes from gossip `absorb_facts` (memory). The fold's `consumed`, `seen` and `next_seq` are bound to the file it opened.
11. Connections: a line is "this node's" when `op.node_id` is the writer's node id **or** the fact's source name is the path.name the store was opened at (`crates/rdm-node/src/storage.rs:127-132`); current view `:136-145`, history `:148-157`, `connection_history` `:206-219`. The op is therefore read, not just carried.
12. Fabric: exactly one `fabric-record` key per root, refused by name otherwise (`fabric_storage.rs:235-242`).
13. Runtime rows: key `node_id--incarnation` (`storage.rs:653-657`, `:1185`); put skips an equal latest row (`:1176`).

**Formats and keys** (a destination fold refuses a format it does not list, `store_log.rs:144-175`)
14. `fabric-record/2` key fabric id (`fabric_storage.rs:196`, `:244`).
15. `mesh-record/3` key mesh id (`storage.rs:263`, `:395`).
16. `node-record/1` key node id, `node-runtime/1`, `node-tag/1` key `<node_id>/<label>` (`storage.rs:1069`, `:1064`, `:1076`, `:1216`).
17. `node-connection/2`, key `source--destination--hex(alpn)--kind` (`crates/rdm-node/src/storage.rs:79`, `:84-90`); `removed` flag `:96-101`.
18. `build-record/1`, key `<build_id>/<record_key>` (`build_state.rs:2243`, `:2262-2279`, `:2588`).

**Layout and locks**
19. Root: `<store_dir>/<fabric_id>/` with `layout` (`r-sr1`), `fabric.jsonl`, `meshes.jsonl`, `nodes.jsonl`, `connections.jsonl`, `builds.jsonl`, `locks/in-flight/<mesh>`, `locks/name/<name>` (`store_root.rs:4-17`, `:44-70`). Lock anchors `locks/name/<name>.reclaim` (never removed) and temp files `.<name>.<node>-<incarnation>.tmp` (`store_log.rs:887`, `:915`).
20. The fabrics' name index is **above** the root: `<store_dir>/fabrics/<name>.json` (`store_root.rs:7`, `:62`, `:193-197`), one JSON per file `{format, record}` (`record_store.rs:15-19`); not in any `.jsonl`.
21. Locks and the name index are **removed** on release (`store_log.rs:926`, `fabric_storage.rs:252-254`); the five `.jsonl` files never shrink.
22. Data dir, not on the root: own row `<data_dir>/nodes/self.json` (`storage.rs:1144-1152`), attempt contexts `<data_dir>/attempt-context/` (`build_state.rs:2237`, `:2411`), held runtimes `deployment/provider.rs:488`. `StorageBoundaries.builds` is documented as holding "each attempt's context" (`storage_backend.rs:66`), but the file backend keeps those in the data dir.

---

## 6. Readers of a record another writer writes

| # | reader | file:line | reads (written by) |
|---|---|---|---|
| X1 | `TickleClient` | `offline.rs:366` | `connections_to`: every source's connection lines (`crates/rdm-node/src/storage.rs:175-177`) |
| X2 | `ConnectionsWriter::refresh_fleet_directs` | `connections_writer.rs:292-308` | `fleet_directs`: every source's Direct lines (`storage.rs:192-194`) |
| X3 | `ConnectionsWriter::hydrate` | `connections_writer.rs:266-288` | own lines incl. a predecessor birth's at the same path.name (`storage.rs:127-132`) |
| X4 | Fabric record, every admin | `admin/boot.rs:430,451,1016`; `accepted.rs:1395`; `status_rpc.rs:216,307,716`; `record_cache.rs:97,124,126`; `mesh_delete.rs:1803`; `admin/adoption.rs:91` | the fabric-primary |
| X5 | fabric name index | `fabric_storage.rs:74,95` | first claimant of the name |
| X6 | Mesh records | `admin/boot.rs:453,1185,1763`; `admin/reconcile.rs:107,124`; `admin/rows_writer.rs:215,219,254`; `admin/runner.rs:16`; `mesh_delete.rs:878,946`; `record_cache.rs:74,117,119`; `status_rpc.rs:215,306`; `storage.rs:143,844`; `admin/adoption.rs:113,125` | each mesh's primary, or the fabric-primary |
| X7 | in-flight rows | `control_plane.rs:218,924`; `fabric_recovery.rs:101`; `admin/adoption.rs:328` | the claimant (fabric-primary or a mesh-primary) |
| X8 | node records of every mesh | `storage.rs:843-851` (`records`) ← `admin/rows_writer.rs:183,193` (`recorded_births`) ← `admin/boot.rs:455,527,568,934,1153,1174,1762`; `admin/adoption.rs:133,566` | each mesh's primary |
| X9 | node records, point/mesh reads | `admin/mesh_delete.rs:302,435`; `admin/projection.rs:219`; `admin/runner.rs:412`; `cert_renewal.rs:44,180,329`; `node_rows.rs:141,146,161,526,884,944,964,998,1154`; `status_rpc.rs:217`; `views.rs:142` | each mesh's primary |
| X10 | node name locks | `node_rows.rs:959,997,1331`; `admin/recovery.rs:547` | each claimant |
| X11 | tags index | `views.rs:130` | each mesh's primary |
| X12 | runtime rows | `admin/boot.rs:516,554,760,1794,2998`; `admin/adoption.rs:556`; `admin/recovery.rs:66,88`; `admin/rows_writer.rs:17`; `admin/runner.rs:530,1048`; `join.rs:231`; `mesh_delete.rs:1685,1707`; `node_investigation.rs:161` | the admin that launched or first heard the birth |
| X13 | Build facts of every writer | `FileJournal::sync`/`sync_for_read` `build_state.rs:2548-2580` (root read only while `reads_root`) | every admin's receipts and the fabric-primary's decisions |
| X14 | `stored_accepted` | `build_state.rs:2630-2637` | the fabric-primary |

---

## 7. Conflicts with storage.md §2 in today's code

Page text is cited in quotes; each item names the code line. Nothing here is a proposal.

1. **Fabric name claim decided outside the store.** `claim_identity` reads the claim and then
   replaces it with `FileRecords::write` (`fabric_storage.rs:73-89`, `:249-251`); two claimants can
   both read none and both write [inferred]. The index sits above the root (`store_root.rs:193-197`), outside the
   five stores the copy covers, and release removes it (`fabric_storage.rs:252-254`).
2. **Build "insert-and-fail" is decided in process, not by the store.** `publish_accepted`,
   `open_attempt` and `claim_attempt` decide on the in-memory fold under the process-local turn mutex
   (`build_state.rs:2444-2474`) and then append a line that cannot refuse (`:2603`, `:2617`, `:2662`,
   append `:2584-2594`); the trait doc promises insert-and-fail (`:1123`, `:1125`, `:1169`). §2's
   decision record needs "the store's insert-and-fail primitive"; on the file provider only
   `StoreLocks::take` is one (`store_log.rs:884`), and no store trait exposes a generic insert.
3. **Name take-over is several store operations.** Mesh: claim, read, release, claim
   (`storage.rs:142-151`); node: the same (`node_rows.rs:941-949`). A freeze or provider swap between
   them leaves a released lock with no new claimant [inferred]; §2 Freeze requires every
   "already-issued … claim operation" settled to "a resolved durable outcome".
4. **Attempt claim with in-flight row is two operations.** `take_in_flight` then `claim_attempt`
   (`build_state.rs:1184-1189`); `FileJournal` does not override it. A cut between them leaves a taken
   row with no claim fact [inferred].
5. **Observed connection writes are not awaited and their queue is memory.** `owe` queues on an
   in-memory mpsc and returns (`connections_writer.rs:342-346`; consumer `:184-189`); a refused write
   backs off in that task (`:432-475`). §2: "a fact a node observes, a connection event, is appended to
   a pending log in its own data dir and replayed at the release or at its restart". Today a process
   exit loses the queue [inferred].
6. **finish_writes does not cover builds, connections or locks** (§4 F1-F5), and the ordinary node has
   none (F10). §2 Freeze: every node settles "every already-issued append and claim operation".
7. **finish_writes does not resolve an indeterminate append.** `StoreLog::finish_issued_writes` only
   cycles the mutex (`store_log.rs:637-639`); a pending line from a failed fsync (`:740`) stays pending
   until the next append. §2: "An indeterminate operation prevents that acknowledgement."
8. **No admission closure.** Nothing stops a new append or claim from starting after the wait (§4 F7);
   §2 Freeze "first closes admission".
9. **No store API yet appends a line with its original op.** `StoreLog::append` always stamps the
   caller's writer (`store_log.rs:611`, `:703-710`). §2 copies "every line … every writer's included";
   connections ownership reads `op.node_id` (`crates/rdm-node/src/storage.rs:127-132`), so a re-stamped
   copy changes which node a line belongs to [inferred].
10. **Process-global sequence counters outside any owner.** `issued()` (`store_log.rs:438-443`) is keyed
    by file path; a writer moving to a new provider path resumes from the destination fold only
    [inferred: an op reused if the destination lacks the writer's latest line].
11. **Order beyond per key.** The nodes path index depends on cross-key file order
    (`storage.rs:1128-1136`, `:1190-1194`), and the builds fold on `StoredFact.seq` with first-line-wins
    (`build_state.rs:2519`, `:2526`). §2 promises "source order preserved per key" only.
12. **Builds fold bound to its opened file.** `FileJournal` reads the root only while it is the
    fabric-primary (`build_state.rs:2439-2441`, `:2549`); its `consumed`, `seen` and `next_seq`
    (`:2287-2295`) belong to the file it opened. A new owner after a swap must refold [inferred].
13. **Handles set once.** `OnceLock` owners: `ControlPlane.node_rows` (`control_plane.rs:83`),
    `Records.contacts` / `.rows` / `.status` (`admin/records.rs:289,365,375`), the OnceLock slot
    holding the status struct (`admin/boot.rs:1318`), `join_slot` (`:1752`), `declarer.record_mesh`
    (`:1375`); `ConnectionsWriter.Inner.storage` is set at construction (`connections_writer.rs:166-177`).
    e2's "one replaceable access point" must reach each.
14. **Two paths and two outside decorators.** `journal` is cloned directly into six owners and also
    reached through `FabricBuildStateAdapter` → `wiring.builds` → `FramedBuilds` (`admin/boot.rs:386`,
    `:1074-1083`, `:1498`, `:1539`, `:1555`, `:1964`, `:2216`); `wiring.fabric_storage`
    (`admin/boot.rs:385`) and the node's `decorate_storage` (`crates/rdm-node/src/node.rs:470-473`) wrap
    handles in application code.
15. **Closures capturing concrete handles.** `FinishWrites` (`storage_backend.rs:135-145`),
    `OpenConnections` (`:146-152`, captures the root path), and the tasks T1-T5 (§1c).
16. **Writes whose outcome is discarded or not owed.** `admin/adoption.rs:282,358` (release),
    `admin/recovery.rs:284`, `admin/runner.rs:825`, `build_op.rs:415`, `node_config.rs:135`,
    `node_properties.rs:108`; the runtime-rows task logs a refusal and retries only at the next birth
    change (`admin/boot.rs:1808-1813`); tag moves owed in memory only (`node_rows.rs:91`, `:191-193`).
    §2 Freeze: "Each node holds every write it would make as owed."
