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
