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
    async fn fabric_rows(&self) -> Result<(Option<FabricIdentity>, Option<FabricPointer>), FabricStorageError> {
        self.inner.fabric_rows().await
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
    async fn put_handover(&self, row: &rafka_node_admin_core::fabric_storage::HandoverRow) -> Result<(), FabricStorageError> {
        self.inner.put_handover(row).await
    }
    async fn handovers(&self) -> Result<Vec<rafka_node_admin_core::fabric_storage::HandoverRow>, FabricStorageError> {
        self.inner.handovers().await
    }
    async fn put_seat_transfer(&self, row: &rafka_node_admin_core::fabric_storage::SeatTransferRow) -> Result<(), FabricStorageError> {
        self.inner.put_seat_transfer(row).await
    }
    async fn seat_transfer(&self, holder_mesh: &str, operation: &str) -> Result<Option<rafka_node_admin_core::fabric_storage::SeatTransferRow>, FabricStorageError> {
        self.inner.seat_transfer(holder_mesh, operation).await
    }
    async fn seat_transfers(&self) -> Result<Vec<rafka_node_admin_core::fabric_storage::SeatTransferRow>, FabricStorageError> {
        self.inner.seat_transfers().await
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

/// A file `fabric.storage` on `root` whose committed fabric seat record answers `seat`.
async fn file_writer(root: &rafka_node_admin_core::store_root::StoreRoot, seat: Option<rafka_node_admin_core::fabric_builds::SeatRecord>) -> rafka_node_admin_core::fabric_storage::FileFabricStorage {
    let s = rafka_node_admin_core::fabric_storage::FileFabricStorage::open(root, &rafka_node_admin_core::model::IncarnationId::mint()).await.unwrap();
    if let Some(seat) = seat {
        s.set_seat_source(rafka_node_admin_core::fabric_builds::SeatSource { read: Arc::new(move || seat.clone()), changed: None });
    }
    s
}

/// CONTRACT (R14 item 2: "Writers are exactly the ladder. Fabric-primary: `fabric/`"; item 9:
/// pre-commit behaviour unchanged): on the fabric's store root only the fabric seat holder puts a
/// pointer. N, whose committed fabric seat record names P's live birth, is refused by name, naming
/// the record, and leaves no pointer row; a writer that has no seat source to read is refused by
/// name too. P, whose record is open to it (it names P, or none: Day 0 before any record), puts
/// the pointer and every reader of the root folds it.
#[tokio::test]
async fn only_the_fabric_seat_holder_puts_a_pointer_on_the_store_root() {
    use rafka_node_admin_core::fabric_builds::SeatRecord;
    let d = std::env::temp_dir().join(format!("fabric-pointer-holder-{}", rafka_node_admin_core::model::NodeId::mint()));
    let root = rafka_node_admin_core::store_root::StoreRoot::at(&d);
    let fabric_id = FabricId::mint();
    let moved = SeatRecord::Moved { by: "the fabric seat record names mesh1 000000000001 (birth p) at epoch 3".into(), to: Some("mesh1.admin.1".into()) };
    let (n, unset, p) = (file_writer(&root, Some(moved)).await, file_writer(&root, None).await, file_writer(&root, Some(SeatRecord::Open { holder: Some("mesh1.admin.1".into()) })).await);
    p.put_identity(&FabricIdentity { fabric_id: fabric_id.clone(), name: "fabric1".into() }).await.unwrap();
    let pointer = |b: &str, at: u64| FabricPointer { build_id: BuildId(b.into()), submitted_at_ms: at };
    match n.put_pointer(&pointer("bld-n", 10)).await {
        Err(FabricStorageError::NotSeatHolder { reason, .. }) => assert!(reason.contains("mesh1.admin.1") && reason.contains("epoch 3"), "the refusal names the holder and its record: {reason}"),
        other => panic!("a writer that does not hold the fabric seat puts no pointer: {other:?}"),
    }
    match unset.put_pointer(&pointer("bld-u", 11)).await {
        Err(FabricStorageError::NotSeatHolder { reason, .. }) => assert!(reason.contains("no fabric seat record to read"), "the refusal names the missing seat source: {reason}"),
        other => panic!("a writer with no seat source puts no pointer: {other:?}"),
    }
    assert_eq!(p.fabric().await.unwrap().and_then(|r| r.build_id), None, "no refused pointer left a row");
    p.put_pointer(&pointer("bld-p", 12)).await.expect("the seat holder puts the pointer");
    assert_eq!(n.fabric().await.unwrap().and_then(|r| r.build_id), Some(BuildId("bld-p".into())), "every reader of the root folds the holder's pointer");
    let _ = std::fs::remove_dir_all(&d);
}
