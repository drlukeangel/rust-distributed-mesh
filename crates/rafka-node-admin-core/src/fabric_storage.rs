//! `fabric.storage`: node-admin's Fabric control state behind a storage boundary
//! (fabric-mesh-lifecycle.md §11.1, §13).
//!
//! It holds the Fabric's identity and pointer rows and, once a fabric shutdown begins, the
//! [`FabricShutdown`] record. Every admin holds its own copy: the fabric-primary writes the shutdown
//! when it initiates one, and every admin that learns it persists the same record before it
//! freezes, so losing the fabric-primary and its storage loses nothing. A shutdown is never deleted
//! while its Fabric lives, and an admin that starts on a data dir holding one comes up frozen.
//!
//! The state is three kinds of row, each its own fact written on its own and never read back to be
//! written again: the [`FabricIdentity`] (written once), one [`FabricPointer`] row per move of
//! `Fabric.build_id` (a blind put ordered by the Build's submission time, folded to the newest by
//! every reader), and the [`FabricShutdown`] (first one kept).
//!
//! Two implementations: [`MemoryFabricStorage`] for runs that need no restart survival, and
//! [`FileFabricStorage`], one record per file in the admin's data dir, each write a temp file,
//! fsync and atomic rename. A file this build does not recognise is refused by name, never read as
//! a value.

use async_trait::async_trait;
use crate::model::FabricId;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Mutex;

/// The Fabric record as a reader sees it: the identity row and the newest pointer row folded. It
/// is never written whole.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FabricRecord {
    /// The fabric's minted id.
    pub fabric_id: FabricId,
    /// The fabric's name.
    pub name: String,
    /// The Build the pointer names, when one was accepted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub build_id: Option<crate::build::BuildId>,
}

/// The Fabric's immutable identity row: written once, by Day 0 or by the admin that first hears it,
/// and never rewritten.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FabricIdentity {
    /// The fabric's minted id.
    pub fabric_id: FabricId,
    /// The fabric's name.
    pub name: String,
}

/// One pointer row: `Fabric.build_id` moved to `build_id`. `submitted_at_ms` is the Build's own
/// submission time, the ordering key: a reader folds the rows to the greatest `(submitted_at_ms,
/// build_id)`, so a row written late by a lagging writer never moves the pointer back.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FabricPointer {
    /// The Build the pointer moved to.
    pub build_id: crate::build::BuildId,
    /// When the Build was submitted, in milliseconds since the Unix epoch.
    pub submitted_at_ms: u64,
}

impl FabricPointer {
    fn order(&self) -> (u64, &str) {
        (self.submitted_at_ms, self.build_id.0.as_str())
    }

    fn key(&self) -> String {
        format!("{:020}-{}", self.submitted_at_ms, self.build_id.0)
    }
}

fn newest(rows: impl IntoIterator<Item = FabricPointer>) -> Option<FabricPointer> {
    rows.into_iter().max_by(|a, b| a.order().cmp(&b.order()))
}

fn folded(identity: Option<FabricIdentity>, pointer: Option<FabricPointer>) -> Option<FabricRecord> {
    identity.map(|i| FabricRecord { fabric_id: i.fabric_id, name: i.name, build_id: pointer.map(|p| p.build_id) })
}

/// One seat record this admin held: who held a seat, and from which epoch. Each is its own row
/// (a blind put of its own key); a reader keeps, per seat and mesh, the one that supersedes the
/// others, so a late write never moves a seat back.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SeatRow {
    /// The seat the record is for.
    pub seat: rafka_mesh_entity::Seat,
    /// The seat's holder.
    pub holder: rafka_mesh_entity::SeatHolder,
}

impl SeatRow {
    fn key(&self) -> String {
        format!("{}-{}-{:020}-{}", self.seat.name(), self.holder.mesh, self.holder.epoch, self.holder.node_id)
    }
}

/// The rows that stand: per seat (and per mesh for a mesh primary), the one that supersedes the rest.
fn standing(rows: impl IntoIterator<Item = SeatRow>) -> Vec<SeatRow> {
    let mut held: std::collections::BTreeMap<(rafka_mesh_entity::Seat, String), SeatRow> = Default::default();
    for r in rows {
        let key = (r.seat, if r.seat == rafka_mesh_entity::Seat::MeshPrimary { r.holder.mesh.clone() } else { String::new() });
        match held.get(&key) {
            Some(h) if !r.holder.supersedes(&h.holder) => {}
            _ => {
                held.insert(key, r);
            }
        }
    }
    held.into_values().collect()
}

/// A fabric shutdown in progress. `initiated_by` names the fabric-primary that began it;
/// `initiated_at_ms` is a diagnostic and takes no part in authority or ordering. The record holds no
/// drain progress: every admin derives the freeze barrier (every live admin `Draining`) and runtime
/// disappearance from its own view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FabricShutdown {
    /// The admin that initiated the shutdown.
    pub initiated_by: String,
    /// The node id of that admin.
    pub initiated_by_node_id: String,
    /// When the shutdown was initiated, in milliseconds since the Unix epoch.
    pub initiated_at_ms: u64,
}

/// Why a `fabric.storage` read or write failed.
pub use crate::record_store::StorageError as FabricStorageError;

/// node-admin's Fabric control state: three kinds of row, each written on its own and never read
/// back to be written again.
#[async_trait]
pub trait FabricStorage: Send + Sync {
    /// The identity row and the newest pointer row folded; `None` while no identity is held.
    async fn fabric(&self) -> Result<Option<FabricRecord>, FabricStorageError>;
    /// Hold the Fabric's identity if none is held (insert-and-fail on its key). Returns the identity
    /// now held; a held identity naming another Fabric is returned unchanged for the caller to refuse.
    async fn put_identity(&self, identity: &FabricIdentity) -> Result<FabricIdentity, FabricStorageError>;
    /// Put the pointer row for one move: a blind put of its own key.
    async fn put_pointer(&self, pointer: &FabricPointer) -> Result<(), FabricStorageError>;
    /// The shutdown held, when one is.
    async fn shutdown(&self) -> Result<Option<FabricShutdown>, FabricStorageError>;
    /// Hold `shutdown` if none is held (insert-and-fail on its key): the first shutdown held is
    /// kept, a later one never replaces it, and it is never removed. Returns the record now held.
    async fn put_shutdown(&self, shutdown: &FabricShutdown) -> Result<FabricShutdown, FabricStorageError>;
    /// Put the row for one seat record: a blind put of its own key.
    async fn put_seat(&self, row: &SeatRow) -> Result<(), FabricStorageError>;
    /// The seat records that stand: per seat and mesh, the one that supersedes the others.
    async fn seats(&self) -> Result<Vec<SeatRow>, FabricStorageError>;
}

/// A fabric store held in memory.
#[derive(Debug, Default)]
pub struct MemoryFabricStorage {
    identity: Mutex<Option<FabricIdentity>>,
    pointers: Mutex<Vec<FabricPointer>>,
    shutdown: Mutex<Option<FabricShutdown>>,
    seats: Mutex<Vec<SeatRow>>,
}

impl MemoryFabricStorage {
    /// An empty store.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl FabricStorage for MemoryFabricStorage {
    async fn fabric(&self) -> Result<Option<FabricRecord>, FabricStorageError> {
        let identity = self.identity.lock().unwrap().clone();
        let pointer = newest(self.pointers.lock().unwrap().iter().cloned());
        Ok(folded(identity, pointer))
    }
    async fn put_identity(&self, identity: &FabricIdentity) -> Result<FabricIdentity, FabricStorageError> {
        Ok(self.identity.lock().unwrap().get_or_insert_with(|| identity.clone()).clone())
    }
    async fn put_pointer(&self, pointer: &FabricPointer) -> Result<(), FabricStorageError> {
        let mut rows = self.pointers.lock().unwrap();
        rows.retain(|r| r.key() != pointer.key());
        rows.push(pointer.clone());
        Ok(())
    }
    async fn shutdown(&self) -> Result<Option<FabricShutdown>, FabricStorageError> {
        Ok(self.shutdown.lock().unwrap().clone())
    }
    async fn put_shutdown(&self, shutdown: &FabricShutdown) -> Result<FabricShutdown, FabricStorageError> {
        Ok(self.shutdown.lock().unwrap().get_or_insert_with(|| shutdown.clone()).clone())
    }
    async fn put_seat(&self, row: &SeatRow) -> Result<(), FabricStorageError> {
        let mut rows = self.seats.lock().unwrap();
        rows.retain(|r| r.key() != row.key());
        rows.push(row.clone());
        Ok(())
    }
    async fn seats(&self) -> Result<Vec<SeatRow>, FabricStorageError> {
        Ok(standing(self.seats.lock().unwrap().iter().cloned()))
    }
}

/// The directory inside an admin's data dir.
pub const FABRIC_DIR: &str = "fabric";
const POINTERS_DIR: &str = "fabric/pointers";
const SEATS_DIR: &str = "fabric/seats";
const SEAT_FORMAT: &str = "fabric-seat/1";
const IDENTITY_KEY: &str = "identity";
const SHUTDOWN_KEY: &str = "shutdown";
/// The retired whole-record file (`fabric.json`, format `fabric-record/1`): identity and pointer in
/// one row. A data dir holding it is refused by name.
const RETIRED_RECORD_KEY: &str = "fabric";
const IDENTITY_FORMAT: &str = "fabric-identity/1";
const POINTER_FORMAT: &str = "fabric-pointer/1";
const SHUTDOWN_FORMAT: &str = "fabric-shutdown/1";

/// One row per file under `<data dir>/fabric/`: `identity.json`, `shutdown.json`, and one
/// `pointers/<submitted_at_ms>-<build_id>.json` per pointer move (`crate::record_store`).
#[derive(Debug)]
pub struct FileFabricStorage {
    records: crate::record_store::FileRecords,
    pointers: crate::record_store::FileRecords,
    seats: crate::record_store::FileRecords,
}

impl FileFabricStorage {
    /// Open the store under the admin's own data directory; a directory holding a retired record is
    /// refused by name.
    pub fn open(own_data_dir: &Path) -> Result<Self, FabricStorageError> {
        let records = crate::record_store::FileRecords::open(own_data_dir, FABRIC_DIR)?;
        if records.holds(RETIRED_RECORD_KEY) {
            return Err(FabricStorageError::Unrecognised {
                file: own_data_dir.join(FABRIC_DIR).join("fabric.json").display().to_string(),
                reason: format!("the whole-record format fabric-record/1 is retired; this build reads {IDENTITY_FORMAT} and {POINTER_FORMAT} rows"),
            });
        }
        Ok(Self { records, pointers: crate::record_store::FileRecords::open(own_data_dir, POINTERS_DIR)?, seats: crate::record_store::FileRecords::open(own_data_dir, SEATS_DIR)? })
    }
}

#[async_trait]
impl FabricStorage for FileFabricStorage {
    async fn fabric(&self) -> Result<Option<FabricRecord>, FabricStorageError> {
        let identity = self.records.read(IDENTITY_KEY, IDENTITY_FORMAT)?;
        let pointer = newest(self.pointers.list::<FabricPointer>(POINTER_FORMAT)?);
        Ok(folded(identity, pointer))
    }
    async fn put_identity(&self, identity: &FabricIdentity) -> Result<FabricIdentity, FabricStorageError> {
        self.records.insert(IDENTITY_KEY, IDENTITY_FORMAT, identity).await?;
        self.records.read(IDENTITY_KEY, IDENTITY_FORMAT)?.ok_or_else(|| FabricStorageError::Io { file: "identity.json".into(), reason: "the identity row vanished after its insert".into() })
    }
    async fn put_pointer(&self, pointer: &FabricPointer) -> Result<(), FabricStorageError> {
        self.pointers.write(&pointer.key(), POINTER_FORMAT, pointer).await
    }
    async fn shutdown(&self) -> Result<Option<FabricShutdown>, FabricStorageError> {
        self.records.read(SHUTDOWN_KEY, SHUTDOWN_FORMAT)
    }
    async fn put_shutdown(&self, shutdown: &FabricShutdown) -> Result<FabricShutdown, FabricStorageError> {
        self.records.insert(SHUTDOWN_KEY, SHUTDOWN_FORMAT, shutdown).await?;
        self.records.read(SHUTDOWN_KEY, SHUTDOWN_FORMAT)?.ok_or_else(|| FabricStorageError::Io { file: "shutdown.json".into(), reason: "the shutdown row vanished after its insert".into() })
    }
    async fn put_seat(&self, row: &SeatRow) -> Result<(), FabricStorageError> {
        self.seats.write(&row.key(), SEAT_FORMAT, row).await
    }
    async fn seats(&self) -> Result<Vec<SeatRow>, FabricStorageError> {
        Ok(standing(self.seats.list::<SeatRow>(SEAT_FORMAT)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shutdown() -> FabricShutdown {
        FabricShutdown { initiated_by: "mesh1.admin.1".into(), initiated_by_node_id: "n1".into(), initiated_at_ms: 10 }
    }

    #[tokio::test]
    async fn a_held_shutdown_is_never_replaced_or_removed() {
        for s in [Box::new(MemoryFabricStorage::new()) as Box<dyn FabricStorage>, {
            let d = tempdir();
            Box::new(FileFabricStorage::open(&d).unwrap())
        }] {
            assert_eq!(s.shutdown().await.unwrap(), None);
            assert_eq!(s.put_shutdown(&shutdown()).await.unwrap(), shutdown());
            let other = FabricShutdown { initiated_by: "mesh2.admin.1".into(), ..shutdown() };
            assert_eq!(s.put_shutdown(&other).await.unwrap(), shutdown(), "the first initiation is kept");
        }
    }

    fn seat_row(seat: rafka_mesh_entity::Seat, mesh: &str, node: &str, epoch: u64) -> SeatRow {
        SeatRow {
            seat,
            holder: rafka_mesh_entity::SeatHolder { mesh: mesh.into(), node_id: rafka_mesh_entity::NodeId::parse(node).unwrap(), incarnation: rafka_mesh_entity::IncarnationId(format!("i{epoch}")), epoch },
        }
    }

    /// CONTRACT (R-A2): the seat records that stand are the superseding one per seat and mesh,
    /// whatever order they were written in, and a file store reloads them. What must NOT happen: a
    /// late write of an older record moving a seat back.
    #[tokio::test]
    async fn the_seat_records_that_stand_are_the_superseding_ones_whatever_the_write_order() {
        use rafka_mesh_entity::Seat::{FabricPrimary, MeshPrimary};
        for s in [Box::new(MemoryFabricStorage::new()) as Box<dyn FabricStorage>, Box::new(FileFabricStorage::open(&tempdir()).unwrap())] {
            assert!(s.seats().await.unwrap().is_empty());
            for row in [
                seat_row(FabricPrimary, "mesh1", "100000000000", 3),
                seat_row(FabricPrimary, "mesh2", "200000000000", 5),
                seat_row(FabricPrimary, "mesh1", "100000000000", 1),
                seat_row(MeshPrimary, "mesh1", "100000000000", 1),
                seat_row(MeshPrimary, "mesh2", "200000000000", 2),
            ] {
                s.put_seat(&row).await.unwrap();
            }
            let standing = s.seats().await.unwrap();
            assert_eq!(standing.len(), 3);
            assert!(standing.contains(&seat_row(FabricPrimary, "mesh2", "200000000000", 5)));
            assert!(standing.contains(&seat_row(MeshPrimary, "mesh1", "100000000000", 1)));
            assert!(standing.contains(&seat_row(MeshPrimary, "mesh2", "200000000000", 2)));
        }
        let d = tempdir();
        FileFabricStorage::open(&d).unwrap().put_seat(&seat_row(FabricPrimary, "mesh1", "100000000000", 7)).await.unwrap();
        assert_eq!(FileFabricStorage::open(&d).unwrap().seats().await.unwrap(), vec![seat_row(FabricPrimary, "mesh1", "100000000000", 7)]);
    }

    #[tokio::test]
    async fn the_file_storage_reloads_what_it_wrote_and_refuses_an_unknown_format_by_name() {
        let d = tempdir();
        let s = FileFabricStorage::open(&d).unwrap();
        let fabric = FabricRecord { fabric_id: FabricId::mint(), name: "fabric1".into(), build_id: Some(crate::build::BuildId("bld-2".into())) };
        s.put_identity(&FabricIdentity { fabric_id: fabric.fabric_id.clone(), name: fabric.name.clone() }).await.unwrap();
        s.put_pointer(&FabricPointer { build_id: crate::build::BuildId("bld-2".into()), submitted_at_ms: 20 }).await.unwrap();
        s.put_pointer(&FabricPointer { build_id: crate::build::BuildId("bld-1".into()), submitted_at_ms: 10 }).await.unwrap();
        s.put_shutdown(&shutdown()).await.unwrap();
        let reopened = FileFabricStorage::open(&d).unwrap();
        assert_eq!(reopened.fabric().await.unwrap(), Some(fabric));
        assert_eq!(reopened.shutdown().await.unwrap(), Some(shutdown()));
        std::fs::write(d.join(FABRIC_DIR).join("shutdown.json"), br#"{"format":"fabric-shutdown/9","record":{}}"#).unwrap();
        assert!(matches!(reopened.shutdown().await, Err(FabricStorageError::Unrecognised { .. })));
        std::fs::write(d.join(FABRIC_DIR).join("shutdown.json"), b"not json").unwrap();
        assert!(matches!(reopened.shutdown().await, Err(FabricStorageError::Unrecognised { .. })));
    }

    #[tokio::test]
    async fn the_file_storage_refuses_the_retired_whole_record_by_name() {
        let d = tempdir();
        std::fs::create_dir_all(d.join(FABRIC_DIR)).unwrap();
        std::fs::write(d.join(FABRIC_DIR).join("fabric.json"), br#"{"format":"fabric-record/1","record":{}}"#).unwrap();
        let err = FileFabricStorage::open(&d).unwrap_err();
        assert!(matches!(&err, FabricStorageError::Unrecognised { file, reason } if file.ends_with("fabric.json") && reason.contains("fabric-record/1")), "{err}");
    }

    #[tokio::test]
    async fn the_identity_is_held_once_for_both_stores() {
        for s in [Box::new(MemoryFabricStorage::new()) as Box<dyn FabricStorage>, Box::new(FileFabricStorage::open(&tempdir()).unwrap())] {
            let first = FabricIdentity { fabric_id: FabricId::mint(), name: "fabric1".into() };
            let other = FabricIdentity { fabric_id: FabricId::mint(), name: "fabric2".into() };
            assert_eq!(s.put_identity(&first).await.unwrap(), first);
            assert_eq!(s.put_identity(&other).await.unwrap(), first, "the held identity is never rewritten");
        }
    }

    fn tempdir() -> std::path::PathBuf {
        crate::record_store::tempdir("fabric-storage")
    }
}
