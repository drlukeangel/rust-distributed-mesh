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
