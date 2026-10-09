# rdm-test-runner

Lists every test executable a checkout has built and runs them, one process per stem, behind
`/api/tests/*`. The admin UI's Tests tab reaches it through `RDM_TEST_RUNNER_URL`; the UI itself
starts no process.

Start it next to the UI:

```sh
cargo build -p rdm-test-runner
RDM_TESTS_TREE=/path/to/rdm-checkout RDM_TEST_RUNNER_BIND_ADDR=127.0.0.1:19190 target/debug/rdm-test-runner &
RDM_TEST_RUNNER_URL=http://127.0.0.1:19190 RDM_ADMIN_UI_BIND_ADDR=127.0.0.1:19090 demo/target/debug/rafka-admin-ui
```

- `RDM_TESTS_TREE`: the checkout whose `cargo build --tests --bins` output is run (default `/home/admin/rust-distributed-mesh`).
- `RDM_TESTS_PARALLEL`: concurrent processes (default 8); `POST /api/tests/config {"parallel": n}` changes it.
- `RDM_TESTS_JOB_SECS`: per-process bound for fast stems (default 300; release, container and acceptance x3, canonical x6).
- The `build` action (`POST /api/tests/build`) runs `cargo build --message-format=json --tests --bins` in the tree; the inventory reads executables from its artifact list and each test from `<exe> --list --format terse`.
- Each child runs in its own process group with `RDM_NODE_ADMIN_API_BASE`, `RDM_EVIDENCE_DIR` and the UI's own settings removed from its env, the gate's cadence for fast stems (release-only stems at production windows, selectable per run), and its own `RDM_ARTIFACTS_DIR` under `<tree>/target/ui-test-runs/<run>/`.
