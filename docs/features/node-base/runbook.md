# node-base — runbook

## Failure modes

### Mode 1 — One bug fix requires editing 4 main.rs files

**Cause:** Substrate code drifted back out of node-base into the per-binary mains. D-025 violation.

**Recovery:** Refactor the duplicated code back into `node-base::run()` or a new helper in `crates/rafka-node-base/`. Each binary's `main.rs` returns to ~10 lines.

### Mode 2 — All 4 binaries fail to compile after a node-base change

**Cause:** Public API of `node_base` changed in a non-backward-compatible way.

**Recovery:** Stage the change as workspace-wide commit; CI gate catches the compile fail before merge. The 4 binaries are tiny — they update in the same commit.

### Mode 3 — `Role` enum gets a new variant but `run_ping_sender` is not spawned for it

**Cause:** Sender-side logic in `node_base::run()` doesn't match the new Role.

**Recovery:** Decide if the new role should ping (Gateway is the only sender today). If yes, add to the `match role {}` block in `run()`. If no, do nothing.

## Cross-references

* Parent: substrate.
* Sibling: [`boot-chain runbook`](../boot-chain/runbook.md).
