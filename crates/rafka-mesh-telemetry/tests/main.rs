//! The crate's one integration-test executable: every file in this directory is a module
//! here, so the crate links once. A stem runs alone as `<exe> <stem>::`.

// Every cell here is delegated with an environment of its own: the shared file's plain `delegated` is unused.
#[allow(dead_code)]
#[path = "../../../tools/test-support/own_process.rs"]
mod own_process;
mod collector_outage;
mod otlp_http_export;
mod runtime_watchdog;
mod span_end_nonblocking;
