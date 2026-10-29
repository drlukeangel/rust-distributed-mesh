# Lead decisions, 2026-10-28 (goal "finish 143, do not stop to ask me questions")

Each entry gives the question, the decision, why (a page, a ruling or a standard), and where it landed. Any of them can be reversed by its number.

| # | Question | Decision | Why | Landed |
|---|---|---|---|---|
| D1 | rdm#102: an unheard mesh-primary's recovery ran on the fabric-primary after mesh2 had elected a primary, so two processes wrote mesh2's rows, and a stale whole-record write put a deleted row back. | The fabric-primary opens the recovery; the mesh's elected primary claims and runs it. The fabric-primary writes a node row only for a mesh with no primary. | Decided by the pages: node-recovery.md:22 (Execution) and :51 (the investigation runs after the election), node-identity-and-state.md:475-479. No ruling. | 16f8e8125 |
| D2 | rdm#107: since rdm#86, a mesh-primary carries another mesh's read up to the fabric-primary; while the fabric-primary is lost, the read ends UNAVAILABLE not-ready, and four fabric_takeover cells panicked on it. | The harness reads as the page answers: a carried read ending not-ready is "not yet" within each wait's bound. No page change. | query.md §1: "carries any other read up to the fabric-primary". The code follows the page. | 8ffd85d69 |
| D3 | rdm#109: a streaming call's 421 after the commit cut is reported UNKNOWN reset. When does the open return? | Arm B: the open is unchanged, and the unserved refusal arrives as the stream's first failure, classified `Unserved` / `UNIMPLEMENTED` like unary. The seat recommended C (the open waits for the request's ack). | No page states when the open returns. The industry model settles it: in gRPC a server-streaming call returns at open, and UNIMPLEMENTED arrives on the first receive. B adds no wait. | lane 1 |
| D4 | rdm#111: cert_renewal's rotation cell raced its holder's join against the issuer's renewal under gate load. | A 48 s issuing TTL, with the holder joining at 50 %. The assertions are unchanged. | A cell window, not product code (span lines in rdm#111). | b7f659fe2 |

## D1: a deleted record is never re-admitted by the root's verify (Q217 over Q207's "the root's answer wins")

- **Red:** lane 3's fast gate at cd5f1c880, `build_op::an_os_stopped_executor_resumed_after_the_next_attempts_claim_lands_no_receipt_of_its_attempt`.
  The resumed birth's refused-caller span at mesh2.admin.2: `own=delete-deleted-birth root=differs decision=re-admit`,
  outcome `mesh-join:join-not-registered`. The mesh-primary held the birth's record Deleted (revision 10, which it wrote);
  the fabric-primary's atlas still named the birth at its path and had not yet heard the delete.
- **Page:** node-investigation.md holds both "A caller whose record is deleted is deleted, its runtime terminated, never
  re-admitted" (Q217) and "verifies its decision at the root with `fabrics.atlas.get` before it acts, and the root's answer
  wins" (Q207). In this race they collide.
- **Decided:** Q217's explicit "never re-admitted" wins: `CallerDecision::verified` keeps `DeleteDeletedBirth` when the root
  names the caller's own birth at the path (3e66a12bd). The root still turns any other decision.
- **For Luke (page text, exact):** node-investigation.md, old "verifies its decision at the root with `fabrics.atlas.get`
  before it acts, and the root's answer wins." new "verifies its decision at the root with `fabrics.atlas.get` before it acts,
  and the root's answer wins, except that a caller whose record it holds deleted stays deleted (Q217)."

## D2: the 2780 failpoint explorer's attempt count

- **Red:** release at 614054e1f, `i143-2780-process`: a delete Build completed at attempt 1 where the cell required 2.
  Spans: claimed at +4 ms, the fabric ready from the Build's own change at +7 ms (the removed node is not a planned member),
  the run planned after it under load 115, so attempt 1 completed it.
- **Page:** states.md:169 names when such a Build completes (the fabric ready again from its own change), not at which attempt.
- **Decided:** the cell accepts 1 or 2 attempts for a run-then-change Build, never more (cd5f1c880). No product change.
