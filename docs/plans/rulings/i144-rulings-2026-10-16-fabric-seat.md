# Q24 — the fabric seat moves without a person when its mesh is lost (Luke 2026-10-16)

**Ruling (Luke 2026-10-16, Q24: A to the six page lines, with the a1 architect's five changes
taken; "Record Q24 and proceed with the first packet before implementation").**

- The fabric-primary candidate set is every ReadyForTraffic node-admin on the backbone; the seat
  stays in its holder's mesh while that mesh has a Ready node-admin; if only Pending admins survive
  the seat waits.
- For the fabric seat alone: unreached by the tickle ladder, no surviving admin able to inspect the
  exact runtime, the hold expired, the seat moves by the backbone rule. Every mesh-primary that
  notices tells the winner (`node.fabric-primary.update`); the winner writes the seat row at the next
  revision once; repeats answer `AlreadyApplied`; a conflicting proposal is rejected or re-resolved
  against the committed row.
- The takeover commits only through the store's authority fence: every fabric write and Build claim
  carries the writer's seat revision; the store validates it against the current committed seat
  revision, rejects every stale one, and serializes the takeover with writes and claims so no
  old-authority operation commits afterwards. The winner begins recovery only after the fence has
  committed; if the store cannot provide it, the takeover stays held.
- The lost mesh's node-admin is re-created by the new fabric-primary (mesh investigation's
  fully-crashed arm): over the inherited data dir only with exit proof; otherwise over a newly
  allocated data dir with a new endpoint key, `inherited = false` (R-FM2); an expired hold and no
  inspection establish no ownership of the old dir.
- Once that admin is Ready, the committed handover of §4.4 names it as the successor and the seat
  returns home. Fabric recovery by a person is only for every node-admin of every mesh lost.
- Pages changed 2026-10-16: elections §1, §4.1, §4.3, §4.4, §5, §6; node-admin-lifecycle §4.4;
  fabric-recovery.md. Record: i144 e6.

## The first packet, before any code

1. The lost mesh's attempt rows (`AttemptOpened(build_id, mesh, attempt)`, insert-and-fail) are
   writable by a mesh-primary of another mesh when the lost mesh's admins are all gone.
2. The store fence exists or what it takes: the seat revision on every fabric write and Build claim,
   validated against the current committed revision, stale refused, takeover serialized with writes
   and claims.
3. R-FM2's newly-allocated-data-dir path serves a node-admin, not only an rpc node.
A packet with a §5 delta on each; arms only after.

## Q28 — the first packet's answer (Luke 2026-10-16): A, the store provides the fence

- Every lane append is an insert-and-fail on its own key, `<lane>/rev-<N>/<seq>`, through
  `insert_claimed`; no append is a check-then-append under a process lock. Insert-and-fail becomes
  atomic across writers for every mesh (item 1's fix).
- `N` is the committed fabric seat revision the writer holds. A takeover writes `seat/<N+1>`
  (per-revision, insert-and-fail: two winners cannot both commit) and seals `N` by claiming `N`'s next
  sequence key. An old-authority writer's next append under `N` meets the seal and is refused by name
  (`RejectedStaleSeat` from the store, spanned): nothing of revision `N` commits afterwards (§5's
  serialization). The winner begins recovery only after `seat/<N+1>` and the seal both stand.
- Reads reconstruct a lane from its sequenced keys in order; a reader never sees a row past a seal.
- Durable format moved; a mesh-audit ratchet for the store layout; every Build writer moves onto the
  primitive; the seat row's blind put goes.
- Item 3 is conformance once that stands: R-FM2's fresh-dir path accepts a node-admin of another
  mesh when run from the fabric lane by the fabric-primary (`recovery.rs:754-763`'s own-mesh filter
  becomes "own mesh, or any mesh with no live node-admin, from the fabric lane").
- Cells, RED first: two processes appending to one lane race on the same seq key and exactly one
  commits; a takeover seals `N` and the old writer's next append is refused stale; two takeovers race
  on `seat/<N+1>` and one commits; a reader reconstructs the lane and sees nothing past the seal; the
  fresh-dir recovery of a lost mesh's node-admin from the fabric lane. Then e6's live cell: the
  fabric-primary's mesh killed, the seat moved, fenced, re-created, handed back, no person.
- Next: the store layout and the seal key's exact shape as the e6.1 packet, before any of it is
  written. This ruling fixes the mechanism, not the bytes.

## Q35 — e6.1's store layout (Luke 2026-10-17): x, one fabric-wide sequence of seat moves; each seat keeps its own revision

- One fabric-wide sequence of seat moves is the serializer; each seat keeps its own revision as data.
- Keys: `seat-moves/<T>.json` on the fabric's store root, `T` a 20-digit zero-padded fabric-wide
  sequence, written through `insert_claimed` at exactly `max(T)+1`. A writer folds the log through
  `T-1` before it inserts, so any concurrent move would have taken that `T` first: one insert
  serializes every seat move in the fabric, and a two-seat move is one row, atomic
  (node-primary-update.md:21, :159).
- Row: `seat-move/1 { seats: [(seat, from_revision, revision, holder, from)], operation | takeover,
  by: exact birth }`. `revision` is that seat's own counter stepping by one; `from_revision` is the one
  the writer folded, and a fold refuses by name a row whose `from_revision` is not the seat's revision
  at `T-1` (the fold's data-validity check; the insert at `max+1` makes it unreachable except by a
  defective writer).
- Per-seat revision stays per seat: `SeatHolder.revision`, `CommittedSeat.revision`,
  `expected_revision`, `RejectedStaleSeat { expected, current }` and gossip's committed revision per
  cohort are unchanged on the wire and on the page.
- Lanes and seals under Q28: `fabric/builds/rev-<R>/<seq>.json` and
  `meshes/<mesh>/builds/rev-<R>/<seq>.json`, `R` the lane's own seat revision, `R` and `seq` 20-digit
  zero-padded; one Build fact per key, `build-fact/1`, through `insert_claimed`; the seal
  `lane-seal/1 { revision, sealed_by, by }` at the first sequence the sealer wins; a writer meeting it
  is refused `RejectedStaleSeat` and appends nothing past it. Whoever commits the move seals, after its
  seat-move row stands: on a planned handover the holder (the seal replaces the process-local
  cut-off), on a takeover the winner; the successor appends only once both stand. Cohort seats are in
  the same log: a restart or replacement moving a cohort primary is one row.
- `in-flight`: the one shared key names its claim's lane key and is void when that key is not in the
  lane below the seal.
- Durable format moved: `build-journal.jsonl` and `seat-transfers/` refused by name; one reseal.
- Cells: two writers racing on `seat-moves/<T>`, exactly one commits and the other folds and
  re-resolves; a two-seat move committed as one row with both revisions stepped; a crash between the
  row and the seal, the successor does not append and the next fold completes the seal; an
  old-authority writer meeting the seal refused stale and nothing past it readable; the lane
  reconstructed in order from its keys.

## Q48 — the seat-move fold's revision check (Luke 2026-10-17): A, strict; every seat change is a row

- A seat-move row's `from_revision` must equal the seat's revision at `T-1` as the fold holds it (a
  seat with no row is at 0). "At least" would let a move computed against a stale view commit.
- Every seat change is one row in `seat-moves/`: kind `election | takeover | handover`. The ordinary
  rule's fill after a proven exit, a takeover and day 0's first seating included; the winner writes
  the row. A seat's revision never advances except by a row.
- e6.1 carries the election-fill row and its cell: a vacancy filled by the rule writes
  `seat-moves/<T>`; a move computed against the pre-fill revision is refused by the fold.

## Q51 — the lost mesh's seat, row writer and node-admin name after a takeover with no exit proof (Luke 2026-10-17): A, with the lost birth's row carrying its proof

- One takeover row moves both seats the lost birth holds: the fabric seat to the winner, and the lost
  mesh's primary seat, when its record names the same birth, to no holder (`seat-move/1`'s holder
  optional; durable format moved). The fabric-primary writes the lost mesh's rows until the re-born
  admin takes the vacant seat by the ordinary rule (an `election` row from the vacated revision).
- The lost birth's row is written `Deleted` with its exit proof `takeover { revision }`, never a
  Deleted that claims nothing: one meaning for Deleted, a birth that holds nothing any more, the reason
  readable. Its data dir is untouched until its return proves exit through the fence (Q28).
- Page lines with the code: elections §1 ("or with the fabric seat, when both are held by the lost
  mesh's node-admin and one takeover row moves both"), §5 ("a living holder keeps its seat unless a
  committed takeover has superseded it"), node-delete.md ("or the committed takeover that superseded
  it"), fabric-recovery.md (the vacating and the name release).
