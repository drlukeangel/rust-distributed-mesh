//! The crate's one integration-test executable: every file in this directory is a module
//! here, so the crate links once. A stem runs alone as `<exe> <stem>::`.

#[cfg(feature = "file-backend")]
mod connection_reconnect;
mod connection_writes;
mod leadership;
#[cfg(feature = "file-backend")]
mod storage_provider;
