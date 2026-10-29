# Load experiments

One entry per load run: what was tested, why, under what conditions, and what came out. The harness is
[testing-load-harness.md](../../eng/testing-load-harness.md); the plan and targets are
[load-testing.md](../proposal/load-testing.md) (Q265). Rows are in
`$HOME/rust-distributed-mesh-deploy/load/<short sha>.jsonl`.

---

## E1: the L1 baseline (2026-10-28)

**What.** Profile L1, the first run of the harness: 2 meshes, 2 node-admins and 8 rpc nodes each (20 nodes),
process provider, release build of `fec9936fe`, defaults (gossip 2 s, staleness 30 s). Measures M1
(bring-up), M2 (creates per second, reported), M5 (one-node create and delete Builds, 20 each) and M8 (idle
CPU and RSS per role over 20 s once settled).

**Why.** Load-testing.md step 1: the baseline every later profile is compared to, and the first number RDM
has ever had for its own cost. Nothing was measured above 20 nodes before this.

**Conditions.** A shared box: lanes 2, 3, 4, 5 and 7 held processes (two Gemini lanes writing tests and
demo examples, two lanes running export-red fixes); load average 11.5 / 27.3 / 29.6 at the start, 8.5 / 23.9
/ 28.3 at the end, on 20 cores. Evidence on (`RDM_EVIDENCE_DIR`). So this is a loaded-box row, not a
quiet-box baseline.

**Outcome.** Every target passed.

| measure | result | target |
|---|---|---|
| M1 bring-up, launch to ready | 1.8 s | 15 s |
| M2 node creates per second | 9.5 /s | reported |
| M5 one node.create Build | p50 264 ms, max 369 ms | 5 s |
| M5 one node.delete Build | p50 80 ms, max 218 ms | 5 s |
| M8 node-admin idle | 0.052 cores, 0.060 GB | 0.10 cores, 0.5 GB |
| M8 rpc node idle | 0.027 cores, 0.028 GB | 0.10 cores, 0.5 GB |

**Read.**
- Bring-up at 1.8 s is far under the R-shape's 7-9 s (i143-prd.md e10.s2). Those numbers were on the debug
  consumer build; this is release. M1 is measured by the harness's wall clock from the first admin's launch
  to the fabric's ready. Cross-checking it against the spans (the Day-0 Build's acceptance to
  `via-on-fabric-ready`) is owed before L2.
- An idle node-admin costs 0.052 cores, inside the 0.10 target but 2.5 times the release baseline CLAUDE.md
  names (about 0.02). The box was loaded and evidence was on, and both inflate it, so this is read again
  on the evidence-off pass (load-testing.md "Conditions") before it is called a finding.
- Memory is low and flat: 60 MB per node-admin, 28 MB per rpc node.

**Next.** E2: the same profile with evidence off for M6 and M8, then M3, M4, M6 and M7 at L1 (step 2), M6
first and short to read its order of magnitude.
