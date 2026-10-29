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

---

## E2: L1 with no span evidence (2026-10-28)

**What.** Profile L1 again, the same shape and build as E1 but at `549cd1a3b`, with no node writing span
evidence (`RDM_EVIDENCE_DIR` unset: `scripts/rdm-load.sh L1 --no-evidence`, the cell
`load_profile_l1_no_evidence`). Measures M1, M5 and M8; M2 is counted from the spans and so is not
reported on this pass.

**Why.** Load-testing.md "Conditions": M8 (and M6) are read from a pass with evidence off, since the JSONL
writer's own CPU and I/O inflate them. E1's idle node-admin (0.052 cores) was 2.5 times the release
baseline CLAUDE.md names, and E1 named this pass as the check before calling it a finding.

**Conditions.** A shared box: lanes 2 and 7 (Gemini) and lane 5 (37 orphaned processes of an interrupted
landing's estate, still running) held processes; load average 4.96 / 15.53 / 20.64 at the start, on 20
cores. Release build, defaults.

**Outcome.** Every target passed.

| measure | E2 (no evidence) | E1 (evidence) | target |
|---|---|---|---|
| M1 bring-up, launch to ready | 1.7 s | 1.8 s | 15 s |
| M5 one node.create Build | p50 226 ms, max 247 ms | p50 264 ms, max 369 ms | 5 s |
| M5 one node.delete Build | p50 68 ms, max 86 ms | p50 80 ms, max 218 ms | 5 s |
| M8 node-admin idle | 0.044 cores, 0.056 GB | 0.052 cores, 0.060 GB | 0.10 cores, 0.5 GB |
| M8 rpc node idle | 0.022 cores, 0.030 GB | 0.027 cores, 0.028 GB | 0.10 cores, 0.5 GB |

**Read.**
- Evidence costs an idle node-admin about 0.008 cores (15 %) and an rpc node about 0.005; the Build
  latencies drop 10-60 % without it, the max most. E1 and E2 ran on different box loads, so the
  differences are an order of magnitude, not a measurement.
- With evidence off an idle node-admin still costs 0.044 cores, about twice CLAUDE.md's release baseline
  (about 0.02, measured on an empty shell, not a 20-node fabric's admin). It is inside the target. Per
  load-testing.md it is a finding to read as a flame before any number moves: which of an idle admin's
  tasks (gossip ticks, backbone publishes, the topology rebuild, the ladders) take its CPU, read from a
  CPU profile of one idle admin at L1, owed before L2.

**Next.** The idle node-admin's CPU profile (above); then M3, M4, M6 and M7 at L1 (step 2), M6 first.

---

## E3: where an idle node-admin's CPU goes (2026-10-28)

**What.** L1 with evidence off (as E2), at `cd555cd8f`, with a CPU profile of one node-admin and one rpc node
over M8's 20 s idle window: `scripts/rdm-load.sh L1 --no-evidence --perf=<dir>` (`perf record -F 499 -g`;
files in `$HOME/rust-distributed-mesh-deploy/load/perf-idle-cd555cd8f/`). Its M8 rows are marked
`profiled`: the profiler's own cost is in them.

**Why.** E2 left an idle node-admin at 0.044 cores, twice CLAUDE.md's release baseline (about 0.02); E2's
read names its flame as owed before L2.

**Conditions.** Shared box, load average 29.7 / 19.2 / 20.1 at the start (a lane gate ran beside it). The
numbers below are shares of one idle admin's samples (482 samples, about 0.05 cores), not absolute costs.

**Outcome.** M1 2.1 s, M5 create max 238 ms, delete max 85 ms, M8 node-admin 0.053 cores (profiled), rpc
node 0.030; every target passed. The idle node-admin's samples, inclusive:

| where | share |
|---|---|
| the QUIC transport (iroh / noq send, receive, connection tasks) | about 40 % |
| RDM's topology channel receive task (`topology::Channel::join`'s frame task) | 11.7 % |
| of which `topology::View::take_snapshot` cloning every `MeshDigest` (`Backbone::join`) | 5.9 % |
| `topology::DigestView::all` collecting a `Vec<MeshDigest>` | 6.5 % |
| iroh-gossip's plumtree message handling (blake3 message ids) | 5.5 % |
| one node-admin boot task (`start_booted` closure 51) | 6.4 % |

**Read.**
- No single defect: an idle admin's CPU follows the traffic it handles. About half is the transport
  carrying gossip and backbone frames, which scales with the gossip and backbone cadences and the node
  count; that is M7's measure (bytes per second per node), next in step 2.
- RDM's own share is the receive path copying the whole view per frame: `take_snapshot` clones every
  `MeshDigest` and `DigestView::all` builds a fresh `Vec` (together about 12 %). That grows with the node
  count, so L2 and L3 read it again; it is a candidate for a flame-backed change only if those
  profiles show it growing, and any change there is a gossip change (CLAUDE.md: Luke's permission and a
  Jaeger flame).
- 0.02 cores was an empty shell's baseline; an admin of a 20-node fabric at 0.044-0.05 is inside the 0.10
  target. Nothing moves; the profile is repeated at L2.

**Next.** M7 (gossip bytes per second) and M6 at L1, then L2 with the same profile.

---

## E4: gossip cost at L1 (2026-10-28)

**What.** L1 with evidence on, at `6d7aeaf3c` plus M7: the gossip bytes per second each node takes in over a
5-minute steady window once the fabric is ready, from every `rdm.mesh.topology.update.via-frame` span
(one per frame delivered to the node, `bytes` its content), the largest node's total and, per channel
kind, the largest node's share. These are the bytes a node's gossip delivered, not what its transport
carried: repeats and gossip control messages are the transport's own and are not counted.

**Why.** Load-testing.md step 2: M7, with a target of 50 KB/s per node at every profile. E3 found half an
idle node-admin's CPU in the transport carrying gossip, so M7 sizes that traffic.

**Conditions.** Shared box (lanes 2, 5 and 7 busy), load average 6.7 / 20.7 / 21.7 at the start. Release
build, defaults (gossip 2 s, backbone 2 s, staleness 30 s).

**Outcome.** Every target passed.

| measure | result | target |
|---|---|---|
| M1 bring-up | 1.3 s | 15 s |
| M2 creates per second | 12.9 /s | reported |
| M7 largest node, every channel | 30.5 KB/s | 50 KB/s |
| M7 largest node, backbone | 18.5 KB/s | reported |
| M7 largest node, its mesh channel | 12.1 KB/s | reported |
| M5 one node.create / node.delete Build | max 253 ms / 82 ms | 5 s |
| M8 node-admin / rpc node idle (evidence on) | 0.056 / 0.032 cores | 0.10 cores |

**Read.**
- M7 is inside its target at 20 nodes but already at 61 % of it, and the larger share is the backbone
  (gossip.md §3.3: every node-admin holds every mesh complete, loads included). The backbone a node-admin
  takes in grows with the fabric's meshes and nodes, so at L2 (48 nodes) and L3 (100) M7 is the measure
  most likely to cross its target first. That is a prediction for L2 to test, not a finding yet.
- A node's mesh channel (12.1 KB/s) at 10 nodes a mesh is about 2.4 KB per node per 2 s heartbeat.
- M2's 12.9 creates/s and M1's 1.3 s are on a quieter box than E1 (load 6.7 against 11.5).

**Next.** M6 (Node RPC) at L1, then L2 with M7 and the idle profile.

---

## E5: Node RPC against one node-admin, short (2026-10-28)

**What.** L1 with evidence off, at `163ad3911` plus M6's short run: 32 callers in the test process each sending
`node.topology.get` of its own mesh to `mesh2.admin.2` (a non-primary node-admin, the others idle) directly,
back to back, for 10 s. Completions per second, latency p50/p99/max over every completed call, and the
calls that ended without a reply, counted apart.

**Why.** Load-testing.md step 2: M6 first and short, to read its order of magnitude against the 2,000/s
target. The only Node RPC load cell before it (`crates/rdm-transport/tests/flood.rs`) asks 200 round trips in
10 s, about 20/s, which said nothing about the ceiling.

The op is the node's own topology read (query.md §2), not Status: every Status request declares a state,
applies one, or asks a birth to reassert itself (`ProbeNodeState`, the tickle), so a Status load at
thousands a second would measure those effects on the fabric, not the RPC. The read is served from what
the node holds and changes nothing.

**Conditions.** Shared box (lanes 2, 5 and 7 busy). Release build; one client process carrying all 32
callers.

**Outcome.**

| measure | result | target |
|---|---|---|
| M6 completions, 32 callers, 10 s | 7,674 /s | 2,000 /s (load-testing.md, on the cheapest direct op) |
| M6 latency | p50 4 ms, p99 7 ms, max 9 ms | p99 under 50 ms |
| M6 calls ended without a reply | 0 | reported |
| M1 / M5 / M8 | 1.4 s; max 245 / 85 ms; admin 0.041 cores | as E2 |

**Read.**
- One node-admin answers nearly four times the target's rate with a p99 seven times under its bound, and
  drops nothing. Node RPC is not the near ceiling at L1; flood.rs's 20/s was its own pass bound.
- The client may be the limit: 32 callers in one process share one endpoint. A run at 64 and 128 callers,
  and from two client processes, shows whether the 7,674 is the node's or the client's.
- Every reply is the mesh's full topology (10 members); the rate per reply byte is a later read if M6 at
  L2 moves.

**Next.** M6 at 64 and 128 callers and for 60 s (the measure's window), then M3 and M4 at L1.
