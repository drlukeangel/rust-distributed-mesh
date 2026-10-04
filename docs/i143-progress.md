# i143 progress

Initiative: build, prove and export the generic Mesh product in RDM.
Plan: drlukeangel/rafka-v2 `docs/plans/i143-node-rpc-pos-on-RDM.md` (PRD); parity ledger
`docs/plans/i143-transport-parity.md`; work list = rafka-v2 milestone
"i143 — Node RPC + generic Mesh product on RDM" (epics #2730–#2788, stories #2731–#2790).

A fresh session resumes from this file plus the open issues on that milestone.

## Done

| story | issue | RDM merge SHA | what |
|---|---|---|---|

## In progress

- e0.s1 drlukeangel/rafka-v2#2731 — legacy-binary disposition + workspace audit (branch `i143-e0-s1`).

## Blockers

- none

## Notes for the next session

- Workspace builds on Linux only since e0.s1 moved the Windows `E:/` `[patch.crates-io]` block to
  `deployment/dev/windows-iroh-patches.toml`.
- Audit gates live in `tools/mesh-audit` (`cargo test -p rafka-mesh-audit`).
