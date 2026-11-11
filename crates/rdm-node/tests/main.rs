//! The crate's one integration-test executable: every file in this directory is a module
//! here, so the crate links once. A stem runs alone as `<exe> <stem>::`.

/// The binary's process environment, set once, here, before `main` and before any thread exists, as
/// every RDM test binary sets it: span export carries DEBUG (`RDM_OTEL_DEBUG=1`), the per-call Node RPC
/// spans among them (Q441).
#[ctor::ctor]
fn process_env() {
    if std::env::var_os("RDM_OTEL_DEBUG").is_none() {
        std::env::set_var("RDM_OTEL_DEBUG", "1");
    }
}

#[cfg(feature = "file-backend")]
mod connection_reconnect;
mod connection_writes;
#[cfg(feature = "file-backend")]
mod connections_open;
mod leadership;
mod node_hooks;
mod operations;
mod spent_calls;
#[cfg(feature = "file-backend")]
mod storage_copy;
#[cfg(feature = "file-backend")]
mod storage_provider;

#[cfg(feature = "file-backend")]
mod node_records;
