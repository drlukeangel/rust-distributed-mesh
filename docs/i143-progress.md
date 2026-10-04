# i143 progress

Initiative: build, prove and export the generic Mesh product in RDM.
Plan: drlukeangel/rafka-v2 `docs/plans/i143-node-rpc-pos-on-RDM.md` (PRD); parity ledger
`docs/plans/i143-transport-parity.md`; work list = rafka-v2 milestone
"i143 — Node RPC + generic Mesh product on RDM" (epics #2730–#2788, stories #2731–#2790).

A fresh session resumes from this file plus the open issues on that milestone.

## Done

| story | issue | RDM merge SHA | what |
|---|---|---|---|
| e0.s1 | #2731 | `76e6a192e7` | legacy-binary disposition gate + workspace audit (`docs/i143/e0-workspace-audit.md`, `tools/mesh-audit`) |
| e0.s2 | #2732 | `9a4966fee3` | parity scanner `parity-scan` + ledger rows (rafka-v2 #2791 `7ee07f8481`) |
| e0.s4 | #2734 | `ecdd17b892` | dependency-rule check `dep-rules` + CI workflow `.github/workflows/i143-gates.yml` |
| e0.s3 | #2733 | `2e0b27c847` | connections-parity gate in `parity-scan` + ledger rows (rafka-v2 #2792 `5aa2e8a05e`); epic e0 closed |
| e7.s1 | #2773 | `0471b4d6ef` | FIRST RED restart canary (`crates/rafka-test-scenario/tests/node_lifecycle__node_restart.rs`, ignored until e7.s5) + `docs/i143/design.md` contract |
| e7.s2 | #2774 | `504fb7cbc8` | SECOND RED seed scenario + runner (`rafka-scenario`), ignored until e7.s5 |
| e1.s1 | #2736 | `8ef30c8f4b` | `rafka-node-admin-core` model/topology + ProcessTable; admin-ui lifecycle moved behind it |
| e4.s1 | #2754 | `c892769960` | `rafka-mesh-entity`: identity, incarnation lineage, membership, per-slot freshness |
| e5.s1 | #2763 | `7314309cc1` | `rafka-node-rpc-contract`: framing, codes, NodeProtocol, Echo, RpcOutcome |
| e1.s2 | #2737 | `e41c804fe7` | Build intents + deterministic planner (`build.rs`) |
| e5.s2 | #2764 | `8efacf4a24` | sealed catalog + tag ledger + transitional adapters (`catalog.rs`) |
| e1.s3 | #2738 | `4f7217f9ff` | BuildStateAdapter: fold, memory adapter, file journal (`build_state.rs`) |
| e5.s3 | #2765 | `849be45d33` | dispatch + certainty cells (`dispatch.rs`); epic e5 closed |
| e1.s4 | #2739 | `8ff002391f` | control routes submit Build (`http.rs`), traceparent helpers |

## In progress

- e2.s1 drlukeangel/rafka-v2#2743 — MESH_SPAWN_TYPE fabric policy (branch `i143-e2-s1`).

## Blockers

- #2755 (e4.s2) and #2756 (e4.s3) wait on rafka-v2 #2722 (i66.e3 connections build, open). Skip them until it closes.

## Notes for the next session

- Workspace builds on Linux only since e0.s1 moved the Windows `E:/` `[patch.crates-io]` block to
  `deployment/dev/windows-iroh-patches.toml`.
- Audit gates live in `tools/mesh-audit` (`cargo test -p rafka-mesh-audit`).
- Parity gate: `cargo run -p rafka-mesh-audit --bin parity-scan -- --repo ../rafka-v2 --json docs/i143/e0-parity-report.json`.
  The rafka-v2 checkout must have full history (`git fetch --unshallow`).
- Dependency rules: `cargo run -p rafka-mesh-audit --bin dep-rules` (also in `.github/workflows/i143-gates.yml`).
