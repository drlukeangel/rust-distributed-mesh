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

## In progress

- e0.s2 drlukeangel/rafka-v2#2732 — parity scanner `parity-scan` (branch `i143-e0-s2`); ledger PR drlukeangel/rafka-v2#2791 merged (`7ee07f8481`).

## Blockers

- none

## Notes for the next session

- Workspace builds on Linux only since e0.s1 moved the Windows `E:/` `[patch.crates-io]` block to
  `deployment/dev/windows-iroh-patches.toml`.
- Audit gates live in `tools/mesh-audit` (`cargo test -p rafka-mesh-audit`).
- Parity gate: `cargo run -p rafka-mesh-audit --bin parity-scan -- --repo ../rafka-v2 --json docs/i143/e0-parity-report.json`.
  The rafka-v2 checkout must have full history (`git fetch --unshallow`).
