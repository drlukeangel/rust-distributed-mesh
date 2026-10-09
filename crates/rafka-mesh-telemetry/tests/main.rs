//! The crate's one integration-test executable: every file in this directory is a module
//! here, so the crate links once. A stem runs alone as `<exe> <stem>::`.

#[path = "../../../tools/test-support/own_process.rs"]
mod own_process;
mod collector_outage;
mod runtime_watchdog;
