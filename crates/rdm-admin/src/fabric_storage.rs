//! `fabric.storage`: node-admin's Fabric control state behind a storage boundary
//! (fabric-mesh-lifecycle.md §11.1, §13).
//!
//! It holds the Fabric's identity row. The Fabric row carries no pointer to the newest Build (R-BI3): the
//! current Build is read from the Build entities themselves
//! ([`crate::build_state::BuildStateAdapter::current_build`]). `fabric/` on the fabric's store root
//! has one writer, the fabric-primary (R14 item 2: "Fabric-primary: `fabric/`. ... Keys never
//! overlap across writers"): it writes the identity.
//! Every other admin reads it there, and holds the identity it learns before the fabric's first
//! state-commit (its join answer, the Build topic) as its own runtime state, never as a row here.
//!
//! Every seat move of the fabric is one row of ONE fabric-wide sequence (Q35 x):
//! `seat-moves/<T>--<node.id>--<incarnation>.json` on the fabric's store root, `T` a 20-digit
//! zero-padded sequence and the key its writer's birth, so no write replaces another (Q109). The
//! writer folds every earlier move and writes its row at the next `T` ([`commit_seat_move`]); every
//! reader folds the rows in key order and ignores a row that does not follow ([`fold`]); the storage
//! stores and returns what it is given. A move of two seats is one row. A committed node.primary.update or node.fabric-primary.update is one such row
//! ([`SeatMoveRow`], `seat-move/1`), written by the seats' row writer (the holder for the node-admin
//! cohort, the mesh-primary for every other cohort); the takeover of the fabric seat past the hold is
//! one too. Each seat keeps its own revision as data. A resuming executor or a restarted holder reads
//! the move of its operation there ([`SeatTransferRow`] is that move as the handover reads it). The
//! seat-transfer rows under `meshes/<mesh>/seat-transfers/`, the per-seat rows (`fabric/seats/`,
//! `fabric-seat/1`) and the handover stages of the retired fabric.primary.update (`fabric/handovers/`,
//! `fabric-handover/1`) are retired: a store root holding one is refused by name.
//!
//! The Fabric's own state is its [`FabricIdentity`] row (written once). The pointer rows an older build
//! wrote (`fabric/pointers/`, `fabric-pointer/1`) are retired: a store root holding one is refused by name.
//!
//! Two implementations: [`MemoryFabricStorage`] for runs that need no restart survival, and
//! [`FileFabricStorage`], one record per file on the fabric's store root, each write a temp file,
//! fsync and atomic rename. A file this build does not recognise is refused by name, never read as
//! a value.

use async_trait::async_trait;
use crate::model::FabricId;
use serde::{Deserialize, Serialize};

use std::sync::Mutex;

pub use rdm_core::model::FabricIdentity;

/// What a claim of the fabric's name wrote.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FabricClaim {
    /// The identity row was written: the key was free.
    Written,
    /// This Fabric's own identity row stands: nothing is written (Day 0's claim met again by the
    /// fabric-primary's row writer, a restart, a successor fabric-primary).
    Held,
}

/// Another Fabric holds the fabric's name: nothing is written (node-identity-and-state.md §2.8, `NameHeld`
/// naming a different FabricId; R-FN1: the name's claim at `<store_dir>/fabrics/<name>`, or the
/// identity row at the fabric's key).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FabricNameHeld {
    /// The fabric name claimed.
    pub name: String,
    /// The Fabric whose identity row holds the key.
    pub by: FabricId,
}

impl std::fmt::Display for FabricNameHeld {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "the fabric name {} is held by Fabric {}: fabric.storage holds that Fabric's claim of the name, and a held claim is never rewritten; a deleted fabric releases it", self.name, self.by)
    }
}

impl std::error::Error for FabricNameHeld {}

/// What a claim of `identity` meets in `held`, the identity row at the fabric's key.
fn claim_fabric_against(held: Option<&FabricIdentity>, identity: &FabricIdentity) -> Result<FabricClaim, FabricNameHeld> {
    match held {
        None => Ok(FabricClaim::Written),
        Some(h) if h.fabric_id == identity.fabric_id => Ok(FabricClaim::Held),
        Some(h) => Err(FabricNameHeld { name: identity.name.clone(), by: h.fabric_id.clone() }),
    }
}

/// One seat a seat move moved (`seat-move/1`, retired: decoding kept, nothing reads or writes it).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MovedSeat {
    /// The seat.
    pub seat: rdm_core::seat::transfer::Seat,
    /// The revision the writer folded it at.
    pub from_revision: u64,
    /// Its revision from this move: its own counter, stepped by one.
    pub revision: u64,
    /// Its holder from this move; `None` when the move leaves the seat vacant (i144 Q51: a takeover
    /// vacates the lost mesh's primary seat, which its mesh's next admin fills by the ordinary rule).
    pub holder: Option<rdm_core::handover::HandoverBirth>,
    /// Its holder before this move; `None` for a seat's first seating.
    pub from: Option<rdm_core::handover::HandoverBirth>,
}

/// What moved the seats of one seat move (Q48: every seat change is a row).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum MoveCause {
    /// The ordinary rule seated its winner (elections §3, §4): day 0's first seating, a seat filled
    /// after its holder's proven exit, a seat a holder took again after its successor's exit.
    Election,
    /// A committed node.primary.update or node.fabric-primary.update: the update's identity.
    Handover {
        /// The update's operation identity.
        operation: String,
        /// Its Build.
        build_id: String,
        /// Its attempt.
        attempt: u32,
    },
    /// The takeover of the fabric seat past the hold (elections §5).
    Takeover,
}

/// One row of the fabric's retired seat-move sequence (`seat-move/1`): who is primary is the election's
/// result, recorded as `is_primary` and `is_fabric_primary` on the node rows; this shape is kept so an
/// older store root's rows still decode, and nothing reads or writes them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SeatMoveRow {
    /// Every seat moved, one row however many.
    pub seats: Vec<MovedSeat>,
    /// What moved them.
    pub cause: MoveCause,
    /// The exact birth that wrote the row.
    pub by: rdm_core::handover::HandoverBirth,
}

/// A written seat move of an older store root: its place `t` in the sequence its key names, and its row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeatMove {
    /// Its place in the sequence, from 1.
    pub t: u64,
    /// The row.
    pub row: SeatMoveRow,
}

/// Why a `fabric.storage` read or write failed.
pub use crate::storage_error::StorageError as FabricStorageError;

/// Claim the fabric's name for `identity.fabric_id` and keep `identity` as the Fabric's identity row
/// (node-identity-and-state.md §2.8): the writer reads the claim at the fabric's `(path, name)` (R-FN1:
/// `<store_dir>/fabrics/<name>`) and claims it, then the identity row at the fabric's key (Q109: the
/// storage decides nothing). A free key takes the row (`Written`); this Fabric's own row there writes
/// nothing (`Held`); another Fabric holds it: `FabricNameHeld` naming that Fabric, and nothing of this
/// fabric is written.
pub async fn claim_identity(storage: &dyn FabricStorage, identity: &FabricIdentity) -> Result<Result<FabricClaim, FabricNameHeld>, FabricStorageError> {
    let named = match claim_fabric_against(storage.name_claim(&identity.name).await?.as_ref(), identity) {
        Ok(c) => c,
        Err(held) => return Ok(Err(held)),
    };
    if named == FabricClaim::Written {
        storage.put_name_claim(identity).await?;
    }
    let own = match claim_fabric_against(storage.fabric().await?.as_ref(), identity) {
        Ok(c) => c,
        Err(held) => return Ok(Err(held)),
    };
    if own == FabricClaim::Written {
        storage.put_identity(identity).await?;
    }
    Ok(Ok(if named == FabricClaim::Written || own == FabricClaim::Written { FabricClaim::Written } else { FabricClaim::Held }))
}

/// fabric.delete's release of the fabric's name (R-FN1: "a deleted fabric releases it"): the name's
/// claim goes when it is `identity`'s Fabric's, and another Fabric's claim there is left as it is.
/// Returns whether it went. The hook s59's fabric.delete calls; nothing else releases a fabric's name.
pub async fn release_name(storage: &dyn FabricStorage, identity: &FabricIdentity) -> Result<bool, FabricStorageError> {
    if !storage.name_claim(&identity.name).await?.is_some_and(|h| h.fabric_id == identity.fabric_id) {
        return Ok(false);
    }
    storage.remove_name_claim(&identity.name).await?;
    Ok(true)
}

/// node-admin's Fabric control state: each kind of row written on its own and never read back to be
/// written again.
#[async_trait]
pub trait FabricStorage: Send + Sync {
    /// The Fabric's identity row; `None` while no identity is held.
    async fn fabric(&self) -> Result<Option<FabricIdentity>, FabricStorageError>;
    /// The claim of the fabric name `name` at the fabric's `(path, name)` (R-FN1:
    /// `<store_dir>/fabrics/<name>`, above the per-fabric roots): the Fabric that holds it, if any.
    async fn name_claim(&self, name: &str) -> Result<Option<FabricIdentity>, FabricStorageError>;
    /// Write `identity` as the claim of its name, as given (Q109: storage stores and returns what it is
    /// given); the claim is the writer's ([`claim_identity`]).
    async fn put_name_claim(&self, identity: &FabricIdentity) -> Result<(), FabricStorageError>;
    /// Remove the claim of the fabric name `name`.
    async fn remove_name_claim(&self, name: &str) -> Result<(), FabricStorageError>;
    /// Write `identity` as the Fabric's identity row, as given.
    async fn put_identity(&self, identity: &FabricIdentity) -> Result<(), FabricStorageError>;
}

/// A fabric store held in memory.
#[derive(Debug, Default)]
pub struct MemoryFabricStorage {
    identity: Mutex<Option<FabricIdentity>>,
}

impl MemoryFabricStorage {
    /// An empty store.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl FabricStorage for MemoryFabricStorage {
    async fn fabric(&self) -> Result<Option<FabricIdentity>, FabricStorageError> {
        Ok(self.identity.lock().unwrap().clone())
    }
    // The identity slot is this store's claim of the name too: one fabric per memory store.
    async fn name_claim(&self, name: &str) -> Result<Option<FabricIdentity>, FabricStorageError> {
        Ok(self.identity.lock().unwrap().clone().filter(|i| i.name == name))
    }
    async fn put_name_claim(&self, identity: &FabricIdentity) -> Result<(), FabricStorageError> {
        *self.identity.lock().unwrap() = Some(identity.clone());
        Ok(())
    }
    async fn remove_name_claim(&self, name: &str) -> Result<(), FabricStorageError> {
        let mut held = self.identity.lock().unwrap();
        if held.as_ref().is_some_and(|i| i.name == name) {
            *held = None;
        }
        Ok(())
    }
    async fn put_identity(&self, identity: &FabricIdentity) -> Result<(), FabricStorageError> {
        *self.identity.lock().unwrap() = Some(identity.clone());
        Ok(())
    }
}

/// The directory inside the fabric's store root.
pub const FABRIC_DIR: &str = "fabric";
#[cfg(feature = "file-backend")]
/// The retired pointer rows (`fabric-pointer/1`, one per move of the pointer an older build kept on
/// the Fabric row): a store root holding one is refused by name (R-BI3).
const RETIRED_POINTERS_DIR: &str = "fabric/pointers";
#[cfg(feature = "file-backend")]
const RETIRED_POINTER_FORMAT: &str = "fabric-pointer/1";
#[cfg(feature = "file-backend")]
/// The retired per-seat rows (`fabric-seat/1`): the leadership view is folded from the transfer rows.
const RETIRED_SEATS_DIR: &str = "fabric/seats";
#[cfg(feature = "file-backend")]
const RETIRED_SEAT_FORMAT: &str = "fabric-seat/1";
#[cfg(feature = "file-backend")]
/// The retired stages of fabric.primary.update (`fabric-handover/1`): node.fabric-primary.update
/// leaves one transfer row.
const RETIRED_HANDOVERS_DIR: &str = "fabric/handovers";
#[cfg(feature = "file-backend")]
const RETIRED_HANDOVER_FORMAT: &str = "fabric-handover/1";
#[cfg(feature = "file-backend")]
/// The retired seat-transfer rows under `meshes/<holder mesh>/` (`fabric-seat-transfer/1`): a store
/// root holding one is refused by name.
const REFUSED_TRANSFERS_DIR: &str = "seat-transfers";
#[cfg(feature = "file-backend")]
const REFUSED_TRANSFER_FORMAT: &str = "fabric-seat-transfer/1";
/// Where an older store root holds its seat moves, `seat-moves/<T>--<node.id>--<incarnation>.json`
/// (retired: nothing reads or writes them).
pub const SEAT_MOVES_DIR: &str = "seat-moves";
/// The retired format of one seat move, kept so its rows decode.
pub const SEAT_MOVE_FORMAT: &str = "seat-move/1";
#[cfg(feature = "file-backend")]
const IDENTITY_KEY: &str = "identity";
#[cfg(feature = "file-backend")]
/// The retired whole-record file (`fabric.json`, format `fabric-record/1`): identity and a Build in
/// one row. A data dir holding it is refused by name.
const RETIRED_RECORD_KEY: &str = "fabric";
#[cfg(feature = "file-backend")]
const IDENTITY_FORMAT: &str = "fabric-identity/1";
#[cfg(feature = "file-backend")]
/// A fabric's claim of its name at `<store_dir>/fabrics/<name>` (R-FN1): the claiming Fabric's identity.
const NAME_FORMAT: &str = "fabric-name/1";

#[cfg(feature = "file-backend")]
/// One row per file under `<store root>/fabric/` (`crate::store_root`): `identity.json`
/// (`crate::record_store`).
#[derive(Debug)]
pub struct FileFabricStorage {
    /// `<store_dir>/fabrics/`: the fabrics' name index above the per-fabric roots (R-FN1), where this
    /// fabric's claim of its name is the first of its rows.
    names: crate::record_store::FileRecords,
    records: crate::record_store::FileRecords,
}

#[cfg(feature = "file-backend")]
impl FileFabricStorage {
    /// Open the store under the fabric's store root, written by the process born as `writer`; a
    /// directory holding a retired record is refused by name.
    pub async fn open(root: &crate::store_root::StoreRoot, writer: &crate::model::IncarnationId) -> Result<Self, FabricStorageError> {
        let own_data_dir = root.dir();
        // The name index opens first: held in the scratchpad, it is the first store the fabric's
        // state-commit publishes.
        let names = root.fabric_names(writer).await?;
        let records = root.records(own_data_dir, FABRIC_DIR, writer).await?;
        if records.holds(RETIRED_RECORD_KEY).await? {
            return Err(FabricStorageError::Unrecognised {
                file: own_data_dir.join(FABRIC_DIR).join("fabric.json").display().to_string(),
                reason: format!("the whole-record format fabric-record/1 is retired; this build reads {IDENTITY_FORMAT} rows"),
            });
        }
        refuse_retired(&own_data_dir.join(RETIRED_POINTERS_DIR), &format!("the pointer row format {RETIRED_POINTER_FORMAT} is retired: the Fabric row carries no pointer to the newest Build, and the current Build is read from the builds store")).await?;
        refuse_retired(&own_data_dir.join(RETIRED_SEATS_DIR), &format!("the per-seat row format {RETIRED_SEAT_FORMAT} is retired: the leadership view is folded from the fabric's seat moves under {SEAT_MOVES_DIR}/")).await?;
        refuse_retired(&own_data_dir.join(RETIRED_HANDOVERS_DIR), &format!("the handover stage format {RETIRED_HANDOVER_FORMAT} is retired: node.fabric-primary.update is one seat move under {SEAT_MOVES_DIR}/")).await?;
        let meshes = own_data_dir.join("meshes");
        let transfer_dirs = crate::blocking::blocking_io(move || -> Vec<std::path::PathBuf> {
            let mut dirs: Vec<std::path::PathBuf> = std::fs::read_dir(&meshes).map(|e| e.flatten().map(|e| e.path().join(REFUSED_TRANSFERS_DIR)).filter(|d| d.is_dir()).collect()).unwrap_or_default();
            dirs.sort();
            dirs
        })
        .await;
        for dir in transfer_dirs {
            refuse_retired(&dir, &format!("the seat-transfer row format {REFUSED_TRANSFER_FORMAT} is retired: every seat move is one row of the fabric's sequence under {SEAT_MOVES_DIR}/ ({SEAT_MOVE_FORMAT})")).await?;
        }
        Ok(Self { names, records })
    }

    /// Finishes every write of the fabric's name claim and identity this store has issued
    /// (node-primary-update.md step 3).
    pub async fn finish_issued_writes(&self) {
        self.names.finish_issued_writes().await;
        self.records.finish_issued_writes().await;
    }
}

#[cfg(feature = "file-backend")]
#[async_trait]
impl FabricStorage for FileFabricStorage {
    async fn fabric(&self) -> Result<Option<FabricIdentity>, FabricStorageError> {
        self.records.read(IDENTITY_KEY, IDENTITY_FORMAT).await
    }
    async fn name_claim(&self, name: &str) -> Result<Option<FabricIdentity>, FabricStorageError> {
        self.names.read(name_key(&self.names, name)?, NAME_FORMAT).await
    }
    async fn put_name_claim(&self, identity: &FabricIdentity) -> Result<(), FabricStorageError> {
        self.names.write(name_key(&self.names, &identity.name)?, NAME_FORMAT, identity).await
    }
    async fn remove_name_claim(&self, name: &str) -> Result<(), FabricStorageError> {
        self.names.remove(name_key(&self.names, name)?).await.map(|_| ())
    }
    async fn put_identity(&self, identity: &FabricIdentity) -> Result<(), FabricStorageError> {
        self.records.write(IDENTITY_KEY, IDENTITY_FORMAT, identity).await
    }
}

#[cfg(feature = "file-backend")]
/// One claim of `identity` at `key` of `records`: the filesystem's own atomic link of the finished row
/// to the key, which fails when the key is live, and then the decision against the row standing there
/// (this Fabric's own: `Held`; another Fabric's: refused, naming it). Held before the fabric's first
/// state-commit, the claim is decided again at the publication against the row then standing at the
/// key. A key that is not one path segment is refused by name: it names no row of the store.
/// A store root an older build left rows of a retired format in `dir` on is refused by name, the
/// first row named and `reason` saying what retired it (the pointer rows, R-BI3; the per-seat rows and
/// the handover stages, proposal Part C2). An empty or absent directory holds no row.
async fn refuse_retired(dir: &std::path::Path, reason: &str) -> Result<(), FabricStorageError> {
    let (dir, reason) = (dir.to_path_buf(), reason.to_string());
    crate::blocking::blocking_io(move || {
        let entries = match std::fs::read_dir(&dir) {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(FabricStorageError::Io { file: dir.display().to_string(), reason: e.to_string() }),
        };
        let mut rows: Vec<std::path::PathBuf> = entries.filter_map(|e| e.ok()).map(|e| e.path()).filter(|p| p.extension().is_some_and(|x| x == "json")).collect();
        rows.sort();
        match rows.first() {
            None => Ok(()),
            Some(row) => Err(FabricStorageError::Unrecognised { file: row.display().to_string(), reason }),
        }
    })
    .await
}

#[cfg(feature = "file-backend")]
/// The fabric name `name` as the key of its claim: one path segment, or no key.
fn name_key<'a>(records: &crate::record_store::FileRecords, name: &'a str) -> Result<&'a str, FabricStorageError> {
    if name.is_empty() || name.starts_with('.') || name.contains(['/', '\\']) {
        return Err(FabricStorageError::Io { file: format!("{}/{name}", records.store_name()), reason: format!("the fabric name {name:?} is not one path segment, so it keys no claim of the fabric's name") });
    }
    Ok(name)
}

#[cfg(all(test, feature = "file-backend"))]
mod tests {
    use super::*;

    /// CONTRACT (the made-up words' removal §7 item 4: "`seat-move/1` retired under the durable-format
    /// rule, its decoding kept"): who is primary is the election's result recorded on the node rows, so
    /// nothing reads or writes the fabric's seat moves; a store root an older build left them on opens,
    /// and each of its rows still decodes in the `seat-move/1` shape.
    #[tokio::test]
    async fn an_older_store_roots_move_rows_still_decode_and_it_opens() {
        let d = tempdir();
        let dir = d.join(SEAT_MOVES_DIR);
        std::fs::create_dir_all(&dir).unwrap();
        let birth = rdm_core::handover::HandoverBirth { mesh: "mesh1".into(), node_id: rdm_core::NodeId::parse("100000000000").unwrap(), incarnation: rdm_core::IncarnationId::parse(&rdm_core::fixture_crockford60("p")).unwrap() };
        let row = SeatMoveRow { seats: vec![MovedSeat { seat: rdm_core::seat::transfer::Seat::FabricPrimary, from_revision: 0, revision: 1, holder: Some(birth.clone()), from: None }], cause: MoveCause::Election, by: birth };
        let file = serde_json::json!({ "format": SEAT_MOVE_FORMAT, "record": row });
        std::fs::write(dir.join("00000000000000000001--100000000000--p.json"), serde_json::to_vec(&file).unwrap()).unwrap();
        FileFabricStorage::open(&crate::store_root::StoreRoot::at(&d), &crate::model::IncarnationId::mint()).await.expect("a store root holding retired seat moves opens");
        let read: serde_json::Value = serde_json::from_slice(&std::fs::read(dir.join("00000000000000000001--100000000000--p.json")).unwrap()).unwrap();
        assert_eq!(read["format"], SEAT_MOVE_FORMAT);
        assert_eq!(serde_json::from_value::<SeatMoveRow>(read["record"].clone()).unwrap(), row, "the retired row decodes");
    }

    #[tokio::test]
    async fn the_file_storage_reloads_what_it_wrote_and_refuses_an_unknown_format_by_name() {
        let d = tempdir();
        let s = FileFabricStorage::open(&crate::store_root::StoreRoot::at(&d), &crate::model::IncarnationId::parse(&rdm_core::fixture_crockford60("test")).unwrap()).await.unwrap();
        let fabric = FabricIdentity { fabric_id: FabricId::mint(), name: "fabric1".into() };
        assert_eq!(crate::fabric_storage::claim_identity(&s, &fabric).await.unwrap(), Ok(FabricClaim::Written));
        let reopened = FileFabricStorage::open(&crate::store_root::StoreRoot::at(&d), &crate::model::IncarnationId::parse(&rdm_core::fixture_crockford60("test")).unwrap()).await.unwrap();
        assert_eq!(reopened.fabric().await.unwrap(), Some(fabric));
        std::fs::write(d.join(FABRIC_DIR).join("identity.json"), br#"{"format":"fabric-identity/9","record":{}}"#).unwrap();
        assert!(matches!(reopened.fabric().await, Err(FabricStorageError::Unrecognised { .. })));
        std::fs::write(d.join(FABRIC_DIR).join("identity.json"), b"not json").unwrap();
        assert!(matches!(reopened.fabric().await, Err(FabricStorageError::Unrecognised { .. })));
    }

    #[tokio::test]
    async fn the_file_storage_refuses_the_retired_whole_record_by_name() {
        let d = tempdir();
        std::fs::create_dir_all(d.join(FABRIC_DIR)).unwrap();
        std::fs::write(d.join(FABRIC_DIR).join("fabric.json"), br#"{"format":"fabric-record/1","record":{}}"#).unwrap();
        let err = FileFabricStorage::open(&crate::store_root::StoreRoot::at(&d), &crate::model::IncarnationId::parse(&rdm_core::fixture_crockford60("test")).unwrap()).await.unwrap_err();
        assert!(matches!(&err, FabricStorageError::Unrecognised { file, reason } if file.ends_with("fabric.json") && reason.contains("fabric-record/1")), "{err}");
    }

    /// CONTRACT (proposal Part C2: "The per-seat rows under `fabric/seats` go"; Part D: the handover
    /// stages of fabric.primary.update go; CLAUDE.md: "a reader that does not RECOGNISE a value must
    /// REFUSE with a named reason"): a store root an older build left a per-seat row (`fabric/seats/`,
    /// `fabric-seat/1`) or a handover stage (`fabric/handovers/`, `fabric-handover/1`) on is refused by
    /// name, the row and the retired format named, and is never read as a root with no seat rows.
    #[tokio::test]
    async fn the_file_storage_refuses_a_retired_seat_or_handover_row_by_name() {
        for (dir, file, format) in [("seats", "fabric-primary--00000000000000000007-100000000000.json", "fabric-seat/1"), ("handovers", "fabric-primary-handover_a-sent.json", "fabric-handover/1")] {
            let d = tempdir();
            std::fs::create_dir_all(d.join(FABRIC_DIR).join(dir)).unwrap();
            std::fs::write(d.join(FABRIC_DIR).join(dir).join(file), format!(r#"{{"format":"{format}","record":{{}}}}"#)).unwrap();
            let err = FileFabricStorage::open(&crate::store_root::StoreRoot::at(&d), &crate::model::IncarnationId::parse(&rdm_core::fixture_crockford60("test")).unwrap()).await.unwrap_err();
            assert!(matches!(&err, FabricStorageError::Unrecognised { file: f, reason } if f.ends_with(file) && reason.contains(format)), "{err}");
        }
    }

    /// CONTRACT (R-BI3: "The Fabric row carries no pointer to the newest Build"): the pointer rows
    /// (`fabric/pointers/`, format `fabric-pointer/1`) are retired. A store root an older build left
    /// them on is refused by name, the row and the retired format named, and is never read as a
    /// Fabric with no pointer.
    #[tokio::test]
    async fn the_file_storage_refuses_a_retired_pointer_row_by_name() {
        let d = tempdir();
        std::fs::create_dir_all(d.join(FABRIC_DIR).join("pointers")).unwrap();
        std::fs::write(d.join(FABRIC_DIR).join("pointers").join("00000000000000000010-bld-1.json"), br#"{"format":"fabric-pointer/1","record":{"build_id":"3qcfk1kq47m9","submitted_at_ms":10}}"#).unwrap();
        let err = FileFabricStorage::open(&crate::store_root::StoreRoot::at(&d), &crate::model::IncarnationId::parse(&rdm_core::fixture_crockford60("test")).unwrap()).await.unwrap_err();
        assert!(matches!(&err, FabricStorageError::Unrecognised { file, reason } if file.ends_with("00000000000000000010-bld-1.json") && reason.contains("fabric-pointer/1")), "{err}");
    }

    /// CONTRACT (node-identity-and-state.md §2.8: the fabric-primary claims the fabric's name "at
    /// its `(path, name)` ... idempotent for the same FabricId and `NameHeld` naming
    /// a different one"): a free key takes the identity; the same Fabric claiming again writes nothing;
    /// another Fabric's claim is refused naming the holder, and the held identity is never rewritten.
    /// The memory store and the file store each run their own check.
    #[tokio::test]
    async fn the_fabric_name_is_claimed_once_by_its_writer_for_both_stores() {
        for s in [Box::new(MemoryFabricStorage::new()) as Box<dyn FabricStorage>, Box::new(FileFabricStorage::open(&crate::store_root::StoreRoot::at(&tempdir()), &crate::model::IncarnationId::parse(&rdm_core::fixture_crockford60("test")).unwrap()).await.unwrap())] {
            let first = FabricIdentity { fabric_id: FabricId::mint(), name: "fabric1".into() };
            let other = FabricIdentity { fabric_id: FabricId::mint(), name: "fabric1".into() };
            assert_eq!(crate::fabric_storage::claim_identity(&*s, &first).await.unwrap(), Ok(FabricClaim::Written));
            assert_eq!(crate::fabric_storage::claim_identity(&*s, &first).await.unwrap(), Ok(FabricClaim::Held), "the same Fabric claiming again writes nothing");
            assert_eq!(crate::fabric_storage::claim_identity(&*s, &other).await.unwrap(), Err(FabricNameHeld { name: "fabric1".into(), by: first.fabric_id.clone() }), "another Fabric's claim is refused naming the holder");
            assert_eq!(s.fabric().await.unwrap(), Some(first.clone()), "the held identity is never rewritten");
        }
    }

    /// CONTRACT (R-FN1: a name is unique per (path, kind) for every entity, the fabric included; the
    /// fabric's (path, name) claim sits at the path level, above the per-fabric store roots
    /// (`<store_dir>/fabrics/<name>`), and a second fabric of the same name under that path is refused
    /// `NameHeld { by }`; Q109: the writer reads the claim and writes it; R14 item 1: the per-fabric root is unchanged, the index sits beside it;
    /// CLAUDE.md: a refused claim writes nothing of the entity): under one store_dir, fabric A claims
    /// fabric1 and its state-commit publishes the claim. Fabric B, another FabricId with its own root
    /// under the same store_dir, claims fabric1: refused naming A, and nothing of B is written, neither
    /// at the index nor under B's root, even once B's scratchpad is published. Must NOT happen: a second fabric of one name under one store_dir taking the name because its
    /// identity row lives under its own root.
    // @feature: mesh-lifecycle
    #[tokio::test]
    async fn a_second_fabric_of_a_held_name_under_one_store_dir_is_refused_naming_the_holder() {
        let d = tempdir();
        let store = d.join("store");
        std::fs::create_dir_all(&store).unwrap();
        let fabric = |name: &str| FabricIdentity { fabric_id: FabricId::mint(), name: name.into() };
        let (a, b) = (fabric("fabric1"), fabric("fabric1"));
        let open = |id: &FabricIdentity| {
            let (store, id) = (store.clone(), id.fabric_id.clone());
            async move {
                let root = crate::store_root::StoreRoot::open(&store, &id).unwrap();
                let s = FileFabricStorage::open(&root, &crate::model::IncarnationId::mint()).await.unwrap();
                (root, s)
            }
        };
        let (root_a, store_a) = open(&a).await;
        assert_eq!(crate::fabric_storage::claim_identity(&store_a, &a).await.unwrap(), Ok(FabricClaim::Written), "A claims fabric1");
        root_a.scratchpad().expect("a root with no Fabric row holds a scratchpad").publish().await.unwrap();
        let (root_b, store_b) = open(&b).await;
        let capture = crate::span_capture::Capture::new("i143-3089-fabric-name-index");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        assert_eq!(crate::admin::claim_fabric_name(&store_b, &b, &crate::model::NodeId::mint()).await, Ok(Err(FabricNameHeld { name: "fabric1".into(), by: a.fabric_id.clone() })), "B's claim of fabric1 under the same store_dir is refused naming A");
        let spans = capture.named("rdm.node_admin.fabric.create.via-fabric-row");
        assert_eq!(spans.len(), 1, "{spans:?}");
        assert_eq!((spans[0].attributes.get("outcome").map(String::as_str), spans[0].attributes.get("holder").cloned()), (Some("name-held"), Some(a.fabric_id.to_string())), "the claim's span names the refusal and its holder: {spans:?}");
        eprintln!("span rdm.node_admin.fabric.create.via-fabric-row trace_id={} {:?}", spans[0].trace_id, spans[0].attributes);
        let published_b = root_b.scratchpad().expect("a root with no Fabric row holds a scratchpad").publish().await.unwrap();
        assert_eq!(published_b.written(), 0, "nothing of B is written: {published_b:?}");
        assert!(!store.join(b.fabric_id.to_string()).join(FABRIC_DIR).join("identity.json").exists(), "B's identity row is not written under its root");
        let index: serde_json::Value = serde_json::from_slice(&std::fs::read(store.join("fabrics").join("fabric1.json")).expect("the name's index row is on the store_dir")).unwrap();
        assert_eq!(index["record"]["fabric_id"].as_str(), Some(a.fabric_id.to_string().as_str()), "the index row at <store_dir>/fabrics/fabric1 is A's: {index}");
    }

    /// CONTRACT (R-FN1: "a deleted fabric releases it"; the hook s59's fabric.delete calls): a
    /// fabric's release of its name lets the name go only when the claim is its own: another Fabric's
    /// release of it is refused (`false`) and the claim stands; the holder's release removes it, and
    /// another Fabric then claims the name. The memory store (its identity slot) and the file store
    /// (the index at `<store_dir>/fabrics/<name>`) each run their own check.
    // @feature: mesh-lifecycle
    #[tokio::test]
    async fn a_fabrics_release_lets_go_of_its_own_name_claim_only() {
        let d = tempdir();
        for (s, store) in [(Box::new(MemoryFabricStorage::new()) as Box<dyn FabricStorage>, "memory"), (Box::new(FileFabricStorage::open(&crate::store_root::StoreRoot::at(&d), &crate::model::IncarnationId::mint()).await.unwrap()), "file")] {
            let (first, other) = (FabricIdentity { fabric_id: FabricId::mint(), name: "fabric1".into() }, FabricIdentity { fabric_id: FabricId::mint(), name: "fabric1".into() });
            assert_eq!(crate::fabric_storage::claim_identity(&*s, &first).await.unwrap(), Ok(FabricClaim::Written), "{store}");
            assert!(!crate::fabric_storage::release_name(&*s, &other).await.unwrap(), "{store}: another Fabric's release of the name is refused");
            assert_eq!(crate::fabric_storage::claim_identity(&*s, &other).await.unwrap(), Err(FabricNameHeld { name: "fabric1".into(), by: first.fabric_id.clone() }), "{store}: the claim stands");
            assert!(crate::fabric_storage::release_name(&*s, &first).await.unwrap(), "{store}: the holder's release lets the name go");
            if store == "file" {
                assert!(!d.join(crate::store_root::FABRIC_NAMES).join("fabric1.json").exists(), "the index row is removed");
            }
        }
    }

    fn tempdir() -> std::path::PathBuf {
        crate::test_support::tempdir("fabric-storage")
    }
}
