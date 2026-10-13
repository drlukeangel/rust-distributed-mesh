//! The binary's process environment (`tests/main.rs` `process_env`, rdm-crate-and-test-layout.md
//! section 3): set once, before any thread exists, and never by a cell. A cell whose process needs
//! another value is its own process, started with that value in its environment
//! (`own_process::delegated_with_env`); the binary's ctor runs in that child too, before the cell.

use std::time::Duration;

/// CONTRACT: a cell started as its own process with a staleness floor of its own in its environment
/// runs under that floor: the binary's ctor sets the gate's cadence only where the environment it
/// was started with names none, and every node the cell starts reads the cadence once, from the
/// process (`rafka_mesh_entity::cadence`). What must NOT happen: the child's ctor overwriting the
/// value its parent started it with, so the cell runs under the gate's 3 s floor while its own
/// process was started with another.
#[test]
fn a_cell_started_with_its_own_staleness_floor_runs_under_it() {
    let cell = "a_cell_started_with_its_own_staleness_floor_runs_under_it";
    if crate::own_process::delegated_with_env(module_path!(), cell, &[("RDM_STALENESS_MS", Some(std::ffi::OsStr::new("60000")))]) {
        return;
    }
    assert_eq!(rafka_mesh_entity::cadence::staleness_floor(), Duration::from_secs(60), "the cell's process runs under the floor it was started with");
}
