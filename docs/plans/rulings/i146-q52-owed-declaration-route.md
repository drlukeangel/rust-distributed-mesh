**Q52: B — arch, relayed by Luke, 2026-10-17.** One `Budget::Owed` call per declaration; the owner supplies the receiver (the seat's current holder) and the permitted carrier policy (Q39's carried-route rule: live peers of the holder's mesh, Forward, never a non-primary); the route projection selects the carrier per attempt (§45); the Owed call executes: direct first, and once an attempt sends nothing its later resends take the route the projection now answers, `ViaPeer(carrier)`, every attempt still targeting the same holder. An attempt's deadline or a carrier's failure never ends the call; retry timing schedules attempts, it never expires the obligation. The obligation ends on the holder's answer or proven exit; a seat move has the owner cancel and retarget; owner cancellation stays under §34. The cadence-based declaration loop is deleted. The transport delivery-path change is authorized for this scope (the flame is in the packet). node-rpc.md §43 and §45 state that the owner chooses the route policy while Owed executes its retries.

Cells: direct failure then carried success within one call; a carrier failure without termination; the holder's exit ending the call; a seat move retargeting; and the shutdown red (`fabric_primary::losing_the_fabric_primary_mesh_moves_control`, mesh2.rpc.4 alive after the fabric stop) green by a passing regression, not by the budget change alone.

Why not A: three simultaneous Owed calls replace direct-first with simultaneous delivery and change the ordered requirement rather than implement it; AlreadyApplied tolerates the copies, it does not resolve the mismatch.

---

# Q52 — the dev's packet (lane e4s8, main ac1acf0d)

See the i144 dev's packet of 2026-10-17 in the conversation record: spec (§34, §43, Q39), issue (`declare_loop` re-declares every gossip interval; `owe` resolves its route once, direct only, client.rs:764-815, so `declare_once`'s carried branch never runs), flames `ce8e64625c41bc0f6c3aab20a86d589a`, `616504d14e12cb45c623982c6eb5edf1`, `c7982edde5a0dab2a20618570f6e241d` (Draining then Leaving declared each cadence; direct NotSent Deadline, carried NotSent), delta and arms A/B.
