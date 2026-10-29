//! The crate's one integration-test executable: every file in this directory is a module
//! here, so the crate links once. A stem runs alone as `<exe> <stem>::`.

#[cfg(feature = "file-backend")]
mod connection_reconnect;
mod connection_writes;
#[cfg(feature = "file-backend")]
mod connections_open;
mod leadership;
mod node_hooks;
mod operations;
#[cfg(feature = "file-backend")]
mod storage_copy;
#[cfg(feature = "file-backend")]
mod storage_provider;
