//! fabric.storage functional: `Fabric.build_id` is never moved back by a concurrent writer.
//!
//! Two writers on one admin move the pointer at once: the fabric-primary's acceptance (`point`) and
//! a record heard on the Build topic (`learn`). One is held in the middle of its write while the
//! other completes with the NEWER Build; releasing the held one must leave the newer Build named.

use async_trait::async_trait;
use rafka_node_admin_core::accepted::{AcceptedStore, FabricTopology};
use rafka_node_admin_core::build::BuildId;
use rafka_node_admin_core::build_state::{BuildAccepted, BuildStateAdapter as _, MemoryBuildStateAdapter};
use rafka_node_admin_core::fabric_storage::{FabricIdentity, FabricPointer, FabricRecord, FabricShutdown, FabricStorage, FabricStorageError, MemoryFabricStorage};
use rafka_node_admin_core::model::FabricId;
use std::sync::Arc;
use tokio::sync::Notify;

/// Memory `fabric.storage` whose FIRST pointer write waits, after announcing it has begun, until released.
struct HoldsFirstWrite {
    inner: MemoryFabricStorage,
    begun: Notify,
    release: Notify,
    held: std::sync::atomic::AtomicBool,
}

#[async_trait]
impl FabricStorage for HoldsFirstWrite {
    async fn fabric(&self) -> Result<Option<FabricRecord>, FabricStorageError> {
        self.inner.fabric().await
    }
    async fn put_identity(&self, identity: &FabricIdentity) -> Result<FabricIdentity, FabricStorageError> {
        self.inner.put_identity(identity).await
    }
    async fn put_pointer(&self, pointer: &FabricPointer) -> Result<(), FabricStorageError> {
        if !self.held.swap(true, std::sync::atomic::Ordering::SeqCst) {
            self.begun.notify_one();
            self.release.notified().await;
        }
        self.inner.put_pointer(pointer).await
    }
    async fn shutdown(&self) -> Result<Option<FabricShutdown>, FabricStorageError> {
        self.inner.shutdown().await
    }
    async fn put_shutdown(&self, shutdown: &FabricShutdown) -> Result<FabricShutdown, FabricStorageError> {
        self.inner.put_shutdown(shutdown).await
    }
    async fn put_seat(&self, row: &rafka_node_admin_core::fabric_storage::SeatRow) -> Result<(), FabricStorageError> {
        self.inner.put_seat(row).await
    }
    async fn seats(&self) -> Result<Vec<rafka_node_admin_core::fabric_storage::SeatRow>, FabricStorageError> {
        self.inner.seats().await
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_pointer_move_held_mid_write_never_moves_the_pointer_back_past_a_newer_build() {
    let (older, newer) = (BuildId("bld-older".into()), BuildId("bld-newer".into()));
    let fabric_id = FabricId::mint();
    let storage = Arc::new(HoldsFirstWrite { inner: MemoryFabricStorage::new(), begun: Notify::new(), release: Notify::new(), held: false.into() });
    storage.inner.put_identity(&FabricIdentity { fabric_id: fabric_id.clone(), name: "fabric1".into() }).await.unwrap();
    let builds = MemoryBuildStateAdapter::new();
    for (id, at) in [(&older, 1), (&newer, 2)] {
        builds.publish_accepted(&BuildAccepted { build_id: id.clone(), topology: FabricTopology::root("fabric1", "mesh1"), submitted_change: None, submitted_at_ms: at }).await.unwrap();
    }
    let store = Arc::new(AcceptedStore::new(storage.clone(), "mesh1.admin.1"));

    // The older Build's pointer move starts and is held in the middle of its write.
    let moving_older = tokio::spawn({
        let (store, older) = (store.clone(), older.clone());
        async move { store.point(&older, 1, "held").await.unwrap() }
    });
    storage.begun.notified().await;
    // The newer Build's record is heard and completes meanwhile.
    store.learn(FabricRecord { fabric_id, name: "fabric1".into(), build_id: Some(newer.clone()) }, &builds, "peer").await;
    assert_eq!(store.build_id().await, Some(newer.clone()), "the newer Build is named once it is learned");
    storage.release.notify_one();
    moving_older.await.unwrap();

    assert_eq!(store.build_id().await, Some(newer), "a held, older pointer write lands after the newer one and must not move the pointer back");
}
