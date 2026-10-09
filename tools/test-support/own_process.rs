//! A test that changes process-wide state runs in its own process.
//!
//! One integration-test executable holds every file of its crate, so a cell that installs the
//! global tracing subscriber, initialises evidence telemetry, or sets an environment variable
//! would change what every other cell in the executable sees. Such a cell calls [`delegated`]
//! first. In the executable that was started for the whole crate (or for a stem) `delegated`
//! re-runs exactly that cell in a fresh process of the same executable, asserts that the child
//! ran the cell and that it passed, and returns `true` so the caller returns. In the child it
//! returns `false` and the cell runs with a process of its own.
//!
//! Included by each crate's `tests/main.rs` with `#[path = "../../../tools/test-support/own_process.rs"]`.

use std::process::Command;

/// The child's marker: the libtest name of the one cell it was started for.
const CHILD: &str = "RDM_TEST_OWN_PROCESS";

/// `true` when the cell was run by a child process and passed (the caller returns); `false` when
/// this process is the child and the cell should run. Panics, naming the cell and carrying the
/// child's output, when the child did not run exactly one passing test.
///
/// Call as `own_process::delegated(module_path!(), "<fn name>")` as the first statement.
pub fn delegated(module_path: &str, test: &str) -> bool {
    let module = module_path.split_once("::").map(|(_, m)| m).unwrap_or("");
    let full = if module.is_empty() { test.to_string() } else { format!("{module}::{test}") };
    if std::env::var(CHILD).is_ok_and(|v| v == full) {
        return false;
    }
    let exe = std::env::current_exe().expect("this test executable's own path");
    let out = Command::new(exe)
        .args([full.as_str(), "--exact", "--nocapture", "--test-threads=1"])
        .env(CHILD, &full)
        .output()
        .unwrap_or_else(|e| panic!("{full}: could not start its own-process child: {e}"));
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    print!("{stdout}");
    eprint!("{stderr}");
    assert!(
        out.status.success() && stdout.contains("test result: ok. 1 passed"),
        "{full}: its own-process child ({}) did not run exactly this cell to a pass; the child's output is above",
        out.status
    );
    true
}
