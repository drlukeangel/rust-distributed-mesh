# Rulings index — one number, one question, one asker

One sequence for both streams. **Dev** = the i144 lead (lanes e4s8, e1s8). **Codex** = lane e7s3. A new
packet takes the next free number here before it is written; a collision is renumbered by the lead.

| Q | asked by | initiative | question | ruling | state |
|---|---|---|---|---|---|
| Q19 | Codex | i147 | the first proven receive regression | A | landed `1da16141` |
| Q21 | Codex | i147 | round evidence | — | see file |
| Q24 | Dev | i144 e6 | the fabric seat moves without a person when its mesh is lost | A | landed e6.1 |
| Q28 / Q35 | Dev | i144 e6 | the store fence; one fabric-wide seat-moves sequence | A | landed e6.1 |
| Q34 | Codex | i147 e3 | the CloudEvents envelope | A | landed |
| Q40 | Dev | i144 e5 | carried outcome code (Forward typed causes) | B | landed |
| Q41 | Dev | i144 e5 | the outcome-code page lines | approved | applied |
| Q42 | Dev | i146 | the exit drain (now: the exit's connections delete) | A | landed |
| Q43 | Dev | i144 e5.2 | the constructor library | A | GREEN |
| Q44 | Dev | i146 | `Budget::Owed` | A | landed `4bab74ab` |
| Q45 | Codex | i146 | the climb reset on a cut-off | A | landed |
| Q46 | Codex | i146 | probe exit evidence | A | landed |
| Q47 + Q49 | Codex | i146 / i144 | stop/start streams; applied operation effects | A, A | landed `01e4bd08` |
| Q48 | Dev | i144 e6 | the seat-moves fold is strict | A | built |
| Q50 | Codex | i143 s88 | the container healing cell lifts its cut before the R-FM1 hold | A | in flight |
| Q51 | Dev | i144 e6 | after a takeover, who holds the lost mesh's seat, rows and name | A, `Deleted { exit_proof: takeover }` | landed e6.3 `8b0e58d0` |
| Q52 | Dev | i146 e3 | the owed declaration keeps its carried route in one Owed call | B | landed `0ef6c700` |
| Q53 | Codex | i143 s88 | the successor's status fold published Pending over Ready | A, fix the root | ruled 2026-10-17 |
| Q54 | Codex | i143 s88 | an Applied declared-state row is durable before state-commit | A | landed `8bd6681d`; ratified by Luke 2026-10-17 after the landing |
| Q55 | Dev | i146 e3 | a signal stop is node.stop's own steps; Draining and the linger go | x | landed `0ef6c700`; `Stopped` declared and node.stop one workflow for command and signal `9e977175`; node.mesh.join one primitive `8668cdd8` |


| Q56 | Codex → Dev | i143 s88 | acquisition of held Mesh state after the pre-commit writer ends | A: the existing contracts carry it ([read](../proposal/i144-q56-held-mesh-state-read.md)) | built and landed by i144 (PRD row) |

| Q57 | Codex | i147 e5 | node-admin's own channels: `types` through `Wiring` | x | ruled 2026-10-17; §8 corrected `7304c9b2` |

| Q58 | Dev | i146 e3 | a mesh-primary whose only other member it stopped by command counts its view cut off and refuses that member's start ([packet](../proposal/i146-q58-cut-off-on-a-commanded-stop.md)) | A, in the pages' words ([ruling](i146-q58-cut-off-on-a-commanded-stop.md)) | asked 2026-10-18; ruled 2026-10-18, delegated (ruled by the lead); the dev builds it in e4s8, gossip.md §6 paragraph with the code |

| Q59 | Codex | i147 e5 | app declaration before the Day-0 writer is Ready ([packet](../proposal/i147-q59-join-declaration-before-ready.md)) | x ([ruling](i147-q59-join-declaration-before-ready.md)): the Day-0 admin holds the declaration at the Join and writes it with its first write after it takes the seat; no new writer | asked 2026-10-18 as Q58, renumbered; ruled 2026-10-18 (Luke); built and landed with e5 |

| Q60 | Dev | i146 e2 | a process exit is held by its evidence thread's file creation: what Q42's bound holds ([packet](../proposal/i146-q60-exit-held-by-a-threads-filesystem-call.md)) | asked 2026-10-18 | |

Next free number: **Q61**.

Files: `i143-q50-…`, `i143-q53-…`, `i144-q40-…`, `i144-q51-…`, `i146-q46-…`, `i146-q47-…`, `i146-q52-…`,
`i146-q55-…`, `i146-q58-…`, `i147-q19-…`, `i147-q59-…`, `i147-q21-…`, `i147-q34-…`; the dated `*-rulings-*.md` files hold the earlier
R-series (R-MSG1, R-RX1…, R-FM1/2, R-CT1, R14).
